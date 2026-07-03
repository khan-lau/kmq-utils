package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
	"github.com/wagslane/go-rabbitmq"
)

type Producer struct {
	ctx       *kcontext.ContextNode
	conf      *RabbitConfig
	conn      *rabbitmq.Conn
	publisher *rabbitmq.Publisher

	started  atomic.Bool // 启动状态管理
	draining atomic.Bool // 排水状态管理

	queue     *ksync.LockedRingBuffer[*RabbitMessage] // 消息队列
	queueSize uint                                    // 消息通道大小

	logf klog.AppLogFuncWithTag
	wg   sync.WaitGroup // 用于同步 Close
}

func NewProducer(ctx *kcontext.ContextNode, queueSize uint, conf *RabbitConfig, logf klog.AppLogFuncWithTag) (*Producer, error) {
	// 1. 初始化本地环形缓冲区
	queue, err := ksync.NewLockedRingBuffer[*RabbitMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, RabbitLogTag, "Create rabbit publish queue failed: %s", err.Error())
		}
		return nil, err
	}
	if len(conf.Addrs) == 0 {
		if logf != nil {
			logf(klog.ErrorLevel, RabbitLogTag, "RabbitMQ config addrs is empty")
		}
		return nil, ErrEmptyAddrs
	}

	// 2. 建立物理连接
	rlog := &GoRabbitLogger{logf: logf}
	conn, err := rabbitmq.NewConn(
		fmt.Sprintf("amqp://%s:%s@%s%s", conf.User, conf.Password, conf.Addrs[0], conf.VHost),
		rabbitmq.WithConnectionOptionsLogger(rlog),
	)
	if err != nil {
		return nil, err
	}

	optionFuncs := make([]func(*rabbitmq.PublisherOptions), 0, 8)
	optionFuncs = append(optionFuncs, rabbitmq.WithPublisherOptionsLogger(rlog))
	optionFuncs = append(optionFuncs, rabbitmq.WithPublisherOptionsExchangeName(conf.Producer.Exchange))
	optionFuncs = append(optionFuncs, rabbitmq.WithPublisherOptionsExchangeKind(conf.Producer.WorkType))
	optionFuncs = append(optionFuncs, rabbitmq.WithPublisherOptionsExchangeDurable)
	optionFuncs = append(optionFuncs, rabbitmq.WithPublisherOptionsExchangeDeclare)
	if conf.Producer.ReturnAck {
		optionFuncs = append(optionFuncs, rabbitmq.WithPublisherOptionsConfirm)
	}

	publisher, err := rabbitmq.NewPublisher(conn, optionFuncs...)
	if err != nil {
		return nil, err
	}

	subCtx := ctx.NewChild("rabbitmq_producer")
	return &Producer{ctx: subCtx, conn: conn, publisher: publisher, queue: queue, queueSize: queueSize, conf: conf, logf: logf}, nil
}

func (that *Producer) Start() {
	if !that.started.CompareAndSwap(false, true) {
		return
	}

	that.publisher.NotifyReturn(func(r rabbitmq.Return) {
		if that.logf != nil {
			that.logf(klog.DebugLevel, RabbitLogTag, "message returned from server: %s", string(r.Body))
		}
	})

	if that.conf.Producer.ReturnAck {
		that.publisher.NotifyPublish(func(c rabbitmq.Confirmation) {
			if that.logf != nil {
				that.logf(klog.DebugLevel, RabbitLogTag, "message confirmed from server. tag: %d, ack: %d", c.DeliveryTag, c.Ack)
			}
		})
	}

	that.wg.Add(1)
	//  后台搬运 (Sender Thread) ---
	go func() {
		defer that.wg.Done()

		buffer := make([]*RabbitMessage, that.queueSize)
		for {
			// 排水核心：DequeueTo 在 queue.Close() 且数据空后返回 0
			n := that.queue.DequeueTo(buffer)
			if n <= 0 {
				return
			}
			for _, msg := range buffer[:n] {
				for {
					err := that.publish(msg)
					if err == nil {
						break // 发送成功
					}

					// // 判定是否强杀
					// if that.ctx.Context().Err() != nil {
					// 	return
					// }

					// 排水期遇到物理链路中断，止损退出
					if that.draining.Load() {
						return
					}

					// 指数退避或固定重试 (Backoff)
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}()

	if that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	// 阻塞等待 Close 信号
	<-that.ctx.Context().Done()

	// 执行排水操作
	that.queue.Close()
}

func (that *Producer) Close() {
	if that.draining.Swap(true) {
		return
	}

	// 1. 发送取消信号给 Start()
	that.ctx.Cancel()

	// 2. 等待搬运协程把 RingBuffer 里的东西搬完并退出
	done := make(chan struct{})
	go func() {
		that.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if that.logf != nil {
			that.logf(klog.InfoLevel, RabbitLogTag, "RabbitMQ buffer drained successfully")
		}
	case <-time.After(20 * time.Second):
		if that.logf != nil {
			that.logf(klog.WarnLevel, RabbitLogTag, "RabbitMQ drain timeout, forcing shutdown")
		}
	}

	// 3. 关闭底层物理资源
	if that.publisher != nil {
		that.publisher.Close()
	}
	if that.conn != nil {
		that.conn.Close()
	}

	that.ctx.Remove()
}

func (that *Producer) Publish(msg *RabbitMessage) bool {
	if that != nil && that.queue != nil {
		return that.queue.Enqueue(msg)
	}
	return false
}

func (that *Producer) PublishMessage(exchange string, router string, message string) bool {
	msg := &RabbitMessage{
		Exchange: exchange,
		Router:   []string{router},
		Body:     []byte(message),
	}
	return that.Publish(msg)
}

func (that *Producer) publish(msg *RabbitMessage) error {
	// 即使主 context 取消了，排水期间我们也给每条消息一点点宽限时间
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()

	err := that.publisher.PublishWithContext( //nolint
		ctx,
		msg.Body,
		msg.Router,
		rabbitmq.WithPublishOptionsExchange(msg.Exchange),
	)
	return err
}
