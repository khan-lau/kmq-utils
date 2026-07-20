package rabbitmq

import (
	"fmt"
	"sync"
	"sync/atomic"

	klog "github.com/khan-lau/kutils/klogger"
	"github.com/wagslane/go-rabbitmq"
)

type Consumer struct {
	conn     *rabbitmq.Conn
	consumer *rabbitmq.Consumer
	conf     *RabbitConfig
	isClosed atomic.Bool
	wg       sync.WaitGroup
	logf     klog.AppLogFuncWithTag
}

func NewConsumer(conf *RabbitConfig, logf klog.AppLogFuncWithTag) (*Consumer, error) {
	logger := &GoRabbitLogger{logf: logf}

	if len(conf.Addrs) == 0 {
		if logf != nil {
			logf(klog.ErrorLevel, RabbitLogTag, 0, "RabbitMQ config addrs is empty")
		}
		return nil, ErrEmptyAddrs
	}

	conn, err := rabbitmq.NewConn(
		fmt.Sprintf("amqp://%s:%s@%s%s", conf.User, conf.Password, conf.Addrs[0], conf.VHost),
		rabbitmq.WithConnectionOptionsLogger(logger),
	)

	if err != nil {
		return nil, err
	}

	consumer, err := rabbitmq.NewConsumer(
		conn,
		conf.Consumer.QueueName,
		// rabbitmq.WithConsumerOptionsConcurrency(2), // 并发数协程数量, go-rabbitmq内部参数
		// rabbitmq.WithConsumerOptionsConsumerName("consumer_1"),// 消费者名称, 不填写会自动生成
		rabbitmq.WithConsumerOptionsRoutingKey(conf.Consumer.KRouterKey), // 路由key, 可以多次设置
		rabbitmq.WithConsumerOptionsExchangeName(conf.Consumer.Exchange),
		rabbitmq.WithConsumerOptionsLogger(logger),
		rabbitmq.WithConsumerOptionsConsumerAutoAck(conf.Consumer.AutoCommit == AUTO_COMMIT_NATIVE), // 自动ACK，需要手动ACK的话，必须设置为false
		rabbitmq.WithConsumerOptionsExchangeKind(conf.Consumer.WorkType),                            // direct, fanout, topic, headers
		rabbitmq.WithConsumerOptionsExchangeDurable,                                                 // Durable true
		// rabbitmq.WithConsumerOptionsExchangeAutoDelete,                   // AutoDelete true
		// rabbitmq.WithConsumerOptionsExchangeInternal,                     // Internal true
		rabbitmq.WithConsumerOptionsExchangeDeclare, // Declare true

		rabbitmq.WithConsumerOptionsQueueDurable, // Durable true
		rabbitmq.WithConsumerOptionsBinding(rabbitmq.Binding{
			RoutingKey: conf.Consumer.KRouterKey,
			BindingOptions: rabbitmq.BindingOptions{
				NoWait:  false,
				Args:    rabbitmq.Table{},
				Declare: true,
			}}),
	)

	if err != nil {
		conn.Close()
		return nil, err
	}

	if conf.OnReady != nil {
		conf.OnReady(true)
	}

	return &Consumer{conn: conn, consumer: consumer, conf: conf, logf: logf}, nil
}

func (that *Consumer) Subscribe() {
	go that.SyncSubscribe()
}

func (that *Consumer) SyncSubscribe() {
	// 1. 明确标记：SyncSubscribe 只要在跑，就代表订阅协程存活
	that.wg.Add(1)
	defer that.wg.Done()

	handler := that.conf.Consumer.MainHandler()
	msgHandler := func(voidObj any, msg *Message) {
		if handler != nil {
			handler(voidObj, msg)
		}
	}

	// 局部 WG：专门追踪此订阅下“正在处理”的消息
	var subWg sync.WaitGroup

	// Run 是阻塞的，由 that.consumer.Close() 触发其返回
	err := that.consumer.Run(func(delivery rabbitmq.Delivery) rabbitmq.Action {
		// 每次消息进来，只在局部 WG 计数
		subWg.Add(1)
		defer subWg.Done()

		if that.isClosed.Load() {
			return rabbitmq.NackRequeue
		}

		msgHandler(that, &Message{Delivery: &delivery})

		// rabbitmq.Ack, 成功处理消息，从队列中移除。
		// rabbitmq.NackDiscard, 失败处理，但错误是永久性的，所以直接丢弃消息。
		// rabbitmq.NackRequeue, 失败处理，但错误是暂时性的，所以重新入队，等待再次处理。
		// rabbitmq.Manual, 手动处理，需要调用msg.Ack()或msg.Nack()
		// if that.conf.Consumer.AutoCommit == AUTO_COMMIT_NATIVE || that.conf.Consumer.AutoCommit == AUTO_COMMIT_CUSTOM {
		if that.conf.Consumer.AutoCommit == AUTO_COMMIT_NATIVE {
			return rabbitmq.Ack
		} else {
			return rabbitmq.Manual
		}
	})

	// --- 重点：Run 返回后（说明 consumer.Close() 被调用了）---
	// 此时不再有新消息进来，但我们需要等待已经在 handler 里的消息跑完
	subWg.Wait()

	if err != nil {
		if !that.isClosed.Load() { // 仅在非主动关闭时记录错误
			that.log(klog.ErrorLevel, "consumer error: %v", err)
		}
	}
	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *Consumer) Close() {
	if !that.isClosed.CompareAndSwap(false, true) {
		return
	}

	// 1. 停掉协议层（断流）
	if that.consumer != nil {
		that.consumer.Close() // 这会导致 Run() 结束阻塞并返回
	}

	// 2. 等待协程退出
	// 此时 SyncSubscribe 内部会先执行 subWg.Wait() 确保业务完了，
	// 然后才执行全局 defer that.wg.Done()
	that.wg.Wait()

	// 3. 彻底物理停机
	if that.conn != nil {
		that.conn.Close()
	}
}

// log 日志记录, 会自动添加 RabbitLogTag
//
//go:inline
func (that *Consumer) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RabbitLogTag, 1, format, args...)
	}
}
