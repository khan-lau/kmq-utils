package rocketmq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	rocket "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
)

type Producer struct {
	ctx        *kcontext.ContextNode
	mqProducer rocket.Producer
	started    atomic.Bool                             // 启动状态管理
	draining   atomic.Bool                             // 排水状态管理
	queue      *ksync.LockedRingBuffer[*RocketMessage] // 消息队列
	queueSize  uint                                    // 消息通道大小
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
	wg         sync.WaitGroup // 用于等待 彻底完成排水
}

func NewProducer(ctx *kcontext.ContextNode, queueSize uint, conf *RocketConfig, logf klog.AppLogFuncWithTag) (*Producer, error) {
	opts := make([]producer.Option, 0, 40)
	if conf.GroupName != "" {
		groupOption := producer.WithGroupName(conf.GroupName)
		opts = append(opts, groupOption)
	}

	if conf.Namespace != "" {
		namespaceOption := producer.WithNamespace(conf.Namespace)
		opts = append(opts, namespaceOption)
	}

	if conf.ClientID != "" {
		clientIdOption := producer.WithInstanceName(conf.ClientID)
		opts = append(opts, clientIdOption)
	}

	var serverOption producer.Option
	if conf.NsResolver {
		serverOption = producer.WithNsResolver(primitive.NewPassthroughResolver(conf.Servers))
	} else {
		namesrv, err := primitive.NewNamesrvAddr(conf.Servers...)
		if err != nil {
			return nil, err
		}

		serverOption = producer.WithNameServer(namesrv)
	}
	opts = append(opts, serverOption)

	if conf.Credentials != nil && conf.Credentials.AccessKey != "" {
		credentialsOption := producer.WithCredentials(primitive.Credentials{
			AccessKey: conf.Credentials.AccessKey,
			SecretKey: conf.Credentials.SecretKey,
		})
		opts = append(opts, credentialsOption)
	}

	retryOption := producer.WithRetry(conf.Producer.Retry)
	timeoutOption := producer.WithSendMsgTimeout(time.Duration(conf.Producer.Timeout))
	queueOption := producer.WithQueueSelector(conf.Producer.QueueSelector)
	interceptorOption := producer.WithInterceptor(conf.Producer.Interceptors...)

	opts = append(opts, retryOption, timeoutOption, queueOption, interceptorOption)

	queue, err := ksync.NewLockedRingBuffer[*RocketMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, rocket_tag, "Create rocketmq publish queue failed: %s", err.Error())
		}
		return nil, err
	}

	rocketProducer, err := rocket.NewProducer(opts...)
	if err != nil {
		return nil, err
	}

	subCtx := ctx.NewChild("rocketmq_producer")

	tProducer := &Producer{
		ctx:        subCtx,
		mqProducer: rocketProducer,
		started:    atomic.Bool{},
		draining:   atomic.Bool{},
		queue:      queue,
		queueSize:  queueSize,
		conf:       conf,
		logf:       logf,
		wg:         sync.WaitGroup{},
	}

	return tProducer, nil
}

func (that *Producer) Start() {
	if !that.started.CompareAndSwap(false, true) {
		return
	}
	go func(mqProducer rocket.Producer) {
		err := mqProducer.Start()
		if err != nil && that.logf != nil {
			that.logf(klog.ErrorLevel, rocket_tag, "Start producer error: %s", err.Error())
		} else if that.logf != nil {
			that.logf(klog.InfoLevel, rocket_tag, "Start producer successfully")
		}
	}(that.mqProducer)

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	time.Sleep(500 * time.Millisecond)

	that.wg.Add(1)

	defer func() {
		that.wg.Done()
		if that.conf.OnExit != nil {
			that.conf.OnExit(nil)
		}
	}()

	buffer := make([]*RocketMessage, that.queueSize)
	for {
		// 排水核心：DequeueTo 在 queue.Close() 且数据空后返回 0
		n := that.queue.DequeueTo(buffer)
		if n <= 0 {
			return
		}

		for _, msg := range buffer[:n] {
			for {
				var sendErr error
				if that.draining.Load() {
					// 排水模式：强制同步发送，确保消息落盘
					sendErr = that.drainPublish(msg)
				} else {
					// 正常模式：根据配置选择异步或同步
					sendErr = that.publish(msg)
				}
				// err := that.publish(msg)
				if sendErr == nil {
					break // 发送成功
				}

				// 判定是否强杀
				if that.ctx.Context().Err() != nil {
					return
				}

				// 排水期遇到物理链路中断，止损退出
				if that.draining.Load() {
					return
				}

				// 指数退避或固定重试 (Backoff)
				time.Sleep(500 * time.Millisecond)
			}
		}
	}

}

func (that *Producer) Close() {
	if that.draining.Swap(true) {
		return
	}

	// 先关闭 RingBuffer，拦截外部注入
	that.queue.Close()

	// 2. 等待搬运协程把 RingBuffer 里的东西搬完并退出
	done := make(chan struct{})
	go func() {
		that.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if that.logf != nil {
			that.logf(klog.InfoLevel, rocket_tag, "RocketMQ buffer drained successfully")
		}
	case <-time.After(20 * time.Second):
		if that.logf != nil {
			that.logf(klog.WarnLevel, rocket_tag, "RocketMQ drain timeout, forcing shutdown")
		}

	}

	// 3. 关闭底层物理资源
	if that.mqProducer != nil {
		_ = that.mqProducer.Shutdown()
	}
	that.ctx.Cancel()
	that.ctx.Remove()
}

//////////////////////////////////////////////////////////////

func (that *Producer) Publish(msg *RocketMessage) bool {
	if that != nil && that.queue != nil {
		return that.queue.Enqueue(msg)
	}
	return false
}

func (that *Producer) PublishMessage(topic string, message []byte) bool {
	msg := &RocketMessage{
		Topic:   topic,
		Message: message,
	}
	return that.Publish(msg)
}

func (that *Producer) PublishData(topic string, message []byte, properties map[string]string) bool {
	msg := &RocketMessage{
		Topic:      topic,
		Message:    message,
		Properties: properties,
	}
	return that.Publish(msg)
}

func (that *Producer) drainPublish(msg *RocketMessage) error {
	mqMessage := primitive.NewMessage(msg.Topic, msg.Message)
	if msg.Properties != nil {
		mqMessage.WithProperties(msg.Properties)
	}
	var err error
	_, err = that.mqProducer.SendSync(that.ctx.Context(), mqMessage)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.ErrorLevel, rocket_tag, "Send message error: %s", err.Error())
		}
		if that.conf.OnError != nil {
			that.conf.OnError(err)
		}
	}
	return err
}

func (that *Producer) publish(msg *RocketMessage) error {
	mqMessage := primitive.NewMessage(msg.Topic, msg.Message)
	if msg.Properties != nil {
		mqMessage.WithProperties(msg.Properties)
	}
	var err error
	if that.conf.Producer.AsyncSend {
		err = that.mqProducer.SendAsync(that.ctx.Context(), func(ctx context.Context, result *primitive.SendResult, err error) {
			if err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, rocket_tag, "Send message error: %s", err.Error())
				}

				if that.conf.OnError != nil {
					that.conf.OnError(err)
				}
			}
		}, mqMessage)
	} else {
		_, err = that.mqProducer.SendSync(that.ctx.Context(), mqMessage)
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, rocket_tag, "Send message error: %s", err.Error())
			}
			if that.conf.OnError != nil {
				that.conf.OnError(err)
			}
		}
	}
	return err
}
