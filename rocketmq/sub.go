package rocketmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rocket "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
	"github.com/khan-lau/kutils/kuuid"
)

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

type PushConsumer struct {
	ctx        *kcontext.ContextNode
	mqConsumer rocket.PushConsumer
	draining   atomic.Bool                       // 正在关闭中, true 表示正在关闭中, false 表示正常运行中, 默认为false
	queue      *ksync.LockedRingBuffer[*Message] // 消息队列
	chanSize   uint                              // 消息通道大小
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
	wg         sync.WaitGroup // 用于等待 SyncSubscribe 彻底完成排水
}

func NewPushConsumer(ctx *kcontext.ContextNode, chanSize uint, conf *RocketConfig, logf klog.AppLogFuncWithTag) (*PushConsumer, error) {
	opts := make([]consumer.Option, 0, 40)

	if conf.GroupName != "" {
		groupOption := consumer.WithGroupName(conf.GroupName)
		opts = append(opts, groupOption)
	}

	if conf.Namespace != "" {
		namespaceOption := consumer.WithNamespace(conf.Namespace)
		opts = append(opts, namespaceOption)
	}

	if conf.ClientID != "" {
		clientIdOption := consumer.WithInstance(conf.ClientID)
		opts = append(opts, clientIdOption)
	}

	var serverOption consumer.Option
	if conf.NsResolver {
		serverOption = consumer.WithNsResolver(primitive.NewPassthroughResolver(conf.Servers))
	} else {
		namesrv, err := primitive.NewNamesrvAddr(conf.Servers...)
		if err != nil {
			return nil, err
		}

		serverOption = consumer.WithNameServer(namesrv)
	}
	opts = append(opts, serverOption)

	if conf.Credentials != nil && conf.Credentials.AccessKey != "" && conf.Credentials.SecretKey != "" {
		credentialsOption := consumer.WithCredentials(primitive.Credentials{
			AccessKey: conf.Credentials.AccessKey,
			SecretKey: conf.Credentials.SecretKey,
		})
		opts = append(opts, credentialsOption)
	}

	reConsumeOption := consumer.WithMaxReconsumeTimes(int32(conf.Consumer.MaxReconsumeTimes))
	batchMaxSizeOption := consumer.WithConsumeMessageBatchMaxSize(conf.Consumer.MessageBatchMaxSize)
	modeOption := consumer.WithConsumerModel(conf.Consumer.Mode)
	orderOption := consumer.WithConsumerOrder(conf.Consumer.Order)
	offsetOption := consumer.WithConsumeFromWhere(conf.Consumer.Offset)
	var timestampOption consumer.Option
	if conf.Consumer.Offset == consumer.ConsumeFromTimestamp {
		// timestampOption = consumer.WithConsumeTimestamp("20131223171201")
		timestampOption = consumer.WithConsumeTimestamp(conf.Consumer.Timestamp)
		opts = append(opts, timestampOption)
	}

	interceptorOption := consumer.WithInterceptor(conf.Consumer.Interceptors...)

	opts = append(opts,
		reConsumeOption, batchMaxSizeOption,
		modeOption, orderOption,
		offsetOption, interceptorOption)

	pushConsumer, err := rocket.NewPushConsumer(opts...)
	if err != nil {
		return nil, err
	}

	queue, err := ksync.NewLockedRingBuffer[*Message](uint64(chanSize))
	if err != nil {
		return nil, err
	}

	// subCtx, SubCancel := context.WithCancel(ctx)
	subCtx := ctx.NewChild("rocketmq_consumer")
	tConsumer := &PushConsumer{
		ctx:        subCtx,
		mqConsumer: pushConsumer,
		draining:   atomic.Bool{}, // 默认为false
		queue:      queue,
		chanSize:   chanSize,
		conf:       conf,
		logf:       logf,
	}

	for _, topic := range conf.Consumer.Topics {
		err = pushConsumer.Subscribe(
			topic,
			consumer.MessageSelector{},
			func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				// 如果处于排水状态，尝试拒绝新消息进入（取决于业务是否允许丢弃）
				if tConsumer.draining.Load() {
					return consumer.ConsumeRetryLater, nil
				}

				for _, msg := range msgs {
					_ = tConsumer.queue.Enqueue(&Message{MessageExt: msg, consumer: nil})
				}

				// consumer.ConsumeSuccess 消费完成
				// consumer.ConsumeRetryLater 稍后重试 消息在当前无法处理，需要稍后再次投递。Broker 会将这条消息放入重试队列，并根据重试策略（例如指数退避）在一段时间后重新投递给你或其他的消费者
				// consumer.Commit 与 RocketMQ 的事务消息（Transactional Message）有关。它们用于生产者（Producer）在发送半消息（Half Message）后，对事务的最终状态进行确认
				// consumer.Rollback 与 RocketMQ 的事务消息（Transactional Message）有关。它们用于生产者（Producer）在发送半消息（Half Message）后，对事务的最终状态进行回滚
				// consumer.SuspendCurrentQueueAMoment 暂停当前队列的消息消费，直到下一次定时器触发。这对于临时处理消息堆积或维护任务很有用
				// 如需要精确的手工ack, 不要使用pushConsumer, 应该使用相对原始的PullConsumer
				return consumer.ConsumeSuccess, nil
			},
		)
		if err != nil {
			return nil, err
		}
	}

	if conf.OnReady != nil {
		conf.OnReady(true)
	}
	return tConsumer, nil
}

func (that *PushConsumer) Subscribe() {
	go that.SyncSubscribe()
}

func (that *PushConsumer) SyncSubscribe() {
	that.wg.Add(1)
	defer that.wg.Done()

	handler := that.conf.Consumer.MainHandler()
	msgHandler := func(voidObj any, msg *Message) {
		if handler != nil {
			handler(voidObj, msg)
		}
	}

	consumerErrChan := make(chan error)
	go func(mqConsumer rocket.PushConsumer) {
		err := mqConsumer.Start()
		if err != nil {
			consumerErrChan <- err
			return
		}
	}(that.mqConsumer)

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			break END_LOOP
		case err := <-consumerErrChan:
			that.log(klog.ErrorLevel, "Start consumer error: %s", err.Error())
			break END_LOOP
		default:
			if msg, ok, isValid := that.queue.TryDequeue(); ok && isValid {
				msgHandler(that, msg)
			} else if !isValid {
				break END_LOOP
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	for _, topic := range that.conf.Consumer.Topics {
		_ = that.mqConsumer.Unsubscribe(topic)
	}

	remainLen := that.queue.Len()
	// 只有在 Start 成功且收到退出信号时才会走到这里
	that.log(klog.InfoLevel, "Draining remaining %s messages...", remainLen)

	remainMsgs := make([]*Message, remainLen)
	that.queue.DequeueToWait(remainMsgs, 5000*time.Millisecond)

	for _, msg := range remainMsgs {
		msgHandler(that, msg)
	}
	that.log(klog.InfoLevel, "Drain completed.")

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}

}

// 关闭消费者, 停止拉取消息, 队列中的消息会继续通过回调函数处理, 直到队列为空
func (that *PushConsumer) Close() {
	// 如果当前值是 false，则瞬间将其改为 true，并返回成功；否则什么都不做，返回失败
	if !that.draining.CompareAndSwap(false, true) {
		return
	}

	for _, topic := range that.conf.Consumer.Topics {
		_ = that.mqConsumer.Unsubscribe(topic)
	}
	that.queue.Close()
	err := that.mqConsumer.Shutdown()
	if err != nil {
		that.log(klog.ErrorLevel, "Shutdown consumer error: %s", err.Error())
	}

	// 这样可以确保主程序在 Close 返回后，数据已经全处理完了
	that.wg.Wait()

	that.ctx.Cancel()
	that.ctx.Remove()
}

// log 日志记录, 会自动添加 RocketLogTag
//
//go:inline
func (that *PushConsumer) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RocketLogTag, 1, format, args...)
	}
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// 订阅者，用于拉取消息, 存在缓存队列, 需要考虑排水机制, 防止服务销毁时, 还有消息未处理
type PullConsumer struct {
	ctx        *kcontext.ContextNode
	mqConsumer rocket.PullConsumer
	draining   atomic.Bool                       // 正在关闭中, true 表示正在关闭中, false 表示正常运行中, 默认为false
	queue      *ksync.LockedRingBuffer[*Message] // 消息队列
	chanSize   uint                              // 消息通道大小
	pullSize   uint                              // 一次拉取的最大消息数量, 不得超过 0x0FFFFFFF 条
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
	wg         sync.WaitGroup // 用于等待 SyncSubscribe 彻底完成排水
}

func NewPullConsumer(ctx *kcontext.ContextNode, chanSize uint, conf *RocketConfig, pullSize uint, logf klog.AppLogFuncWithTag) (*PullConsumer, error) {
	opts := make([]consumer.Option, 0, 40)

	if conf.GroupName != "" {
		groupOption := consumer.WithGroupName(conf.GroupName)
		opts = append(opts, groupOption)
	}

	if conf.Namespace != "" {
		namespaceOption := consumer.WithNamespace(conf.Namespace)
		opts = append(opts, namespaceOption)
	}

	if conf.ClientID != "" {
		clientIdOption := consumer.WithInstance(conf.ClientID)
		opts = append(opts, clientIdOption)
	}

	var serverOption consumer.Option
	if conf.NsResolver {
		serverOption = consumer.WithNsResolver(primitive.NewPassthroughResolver(conf.Servers))
	} else {
		namesrv, err := primitive.NewNamesrvAddr(conf.Servers...)
		if err != nil {
			return nil, err
		}

		serverOption = consumer.WithNameServer(namesrv)
	}
	opts = append(opts, serverOption)

	if conf.Credentials != nil && conf.Credentials.AccessKey != "" && conf.Credentials.SecretKey != "" {
		credentialsOption := consumer.WithCredentials(primitive.Credentials{
			AccessKey: conf.Credentials.AccessKey,
			SecretKey: conf.Credentials.SecretKey,
		})
		opts = append(opts, credentialsOption)
	}

	reConsumeOption := consumer.WithMaxReconsumeTimes(int32(conf.Consumer.MaxReconsumeTimes))
	batchMaxSizeOption := consumer.WithConsumeMessageBatchMaxSize(conf.Consumer.MessageBatchMaxSize)
	modeOption := consumer.WithConsumerModel(conf.Consumer.Mode)
	orderOption := consumer.WithConsumerOrder(conf.Consumer.Order)
	offsetOption := consumer.WithConsumeFromWhere(conf.Consumer.Offset)
	autoCommitOption := consumer.WithAutoCommit(conf.Consumer.AutoCommit == AUTO_COMMIT_NATIVE) // 原生自动提交
	var timestampOption consumer.Option
	if conf.Consumer.Offset == consumer.ConsumeFromTimestamp {
		// timestampOption = consumer.WithConsumeTimestamp("20131223171201")
		timestampOption = consumer.WithConsumeTimestamp(conf.Consumer.Timestamp)
		opts = append(opts, timestampOption)
	}

	interceptorOption := consumer.WithInterceptor(conf.Consumer.Interceptors...)

	opts = append(opts,
		reConsumeOption, batchMaxSizeOption,
		modeOption, orderOption,
		offsetOption, interceptorOption, autoCommitOption)

	pullConsumer, err := rocket.NewPullConsumer(opts...)
	if err != nil {
		return nil, err
	}
	queue, err := ksync.NewLockedRingBuffer[*Message](uint64(chanSize))
	if err != nil {
		return nil, err
	}

	subCtx := ctx.NewChild("rocketmq_consumer")
	tConsumer := &PullConsumer{
		ctx:        subCtx,
		mqConsumer: pullConsumer,
		draining:   atomic.Bool{}, // 默认为false
		queue:      queue,
		pullSize:   pullSize,
		conf:       conf,
		logf:       logf,
	}

	// 在主循环中拉取消息
	for _, topic := range conf.Consumer.Topics {
		err := pullConsumer.Subscribe(topic, consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: "*",
		})
		if err != nil {
			return nil, err
		}

		pullCtx := ctx.NewChild(fmt.Sprintf("rocketmq_%s_runloop_consumer", topic))
		go func(ctx *kcontext.ContextNode) {
		END_LOOP:
			for {
				select {
				case <-pullCtx.Context().Done():
					break END_LOOP
				default:
				}

				msgs, err := tConsumer.pull(ctx, int(pullSize))
				if err != nil {
					tConsumer.log(klog.ErrorLevel, "consumer pull error: %s", err.Error())
				} else {
					for _, msg := range msgs {
						if tConsumer.draining.Load() {
							break
						}
						_ = tConsumer.queue.Enqueue(msg)
					}
				}

			}
		}(pullCtx)
	}

	if conf.OnReady != nil {
		conf.OnReady(true)
	}
	return tConsumer, nil
}

func (that *PullConsumer) pull(ctx *kcontext.ContextNode, maxSize int) ([]*Message, error) {
	uuid1, _ := kuuid.NewV1()
	substr := uuid1.ShortString()
	subCtx := ctx.NewChild("rocketmq_pull_consumer_" + substr)
	defer func() {
		subCtx.Cancel()
		subCtx.Remove()
	}()
	resp, err := that.mqConsumer.Pull(subCtx.Context(), maxSize)
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		return nil, err
	}

	msgs := make([]*Message, 0, len(resp.GetMessages()))
	switch resp.Status {
	case primitive.PullFound:
		// var queue *primitive.MessageQueue
		if len(resp.GetMessages()) <= 0 {
			return []*Message{}, nil
		}
		for _, msg := range resp.GetMessageExts() {
			// queue = msg.Queue
			msgs = append(msgs, &Message{MessageExt: msg, consumer: that})
		}
		// // UpdateOffset更新本地内存中的offset, 还需要PersistOffset持久化到队列
		// err = that.mqConsumer.UpdateOffset(queue, resp.NextBeginOffset)
		// if err != nil {
		// 	return nil, err
		// }

	case primitive.PullNoNewMsg, primitive.PullNoMsgMatched:
		// 没有新数据, 或者没有匹配的消息
		time.Sleep(500 * time.Millisecond)
		// return nil, fmt.Errorf("no pull message, next: %s", resp.NextBeginOffset)
		return []*Message{}, nil
	case primitive.PullBrokerTimeout:
		// 网络超时, 延迟重试
		time.Sleep(500 * time.Millisecond)
		// return nil, fmt.Errorf("pull broker timeout, next: %s", resp.NextBeginOffset)
		return []*Message{}, nil
	case primitive.PullOffsetIllegal:
		// 拉取的offset不合法, 延迟重试
		time.Sleep(500 * time.Millisecond)
		// return nil, fmt.Errorf("pull offset illegal, next: %s", resp.NextBeginOffset)
		return []*Message{}, nil
	default:
		return nil, fmt.Errorf("pull error: %v", resp.Status)
	}

	return msgs, nil
}

func (that *PullConsumer) Subscribe() {
	go that.SyncSubscribe()
}

func (that *PullConsumer) SyncSubscribe() {
	that.wg.Add(1)
	defer that.wg.Done()

	handler := that.conf.Consumer.MainHandler()
	msgHandler := func(voidObj any, msg *Message) {
		if handler != nil {
			handler(voidObj, msg)
		}
	}

	consumerErrChan := make(chan error)
	go func(mqConsumer rocket.PullConsumer) {
		err := mqConsumer.Start()
		if err != nil {
			consumerErrChan <- err
			return
		}
	}(that.mqConsumer)

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			break END_LOOP
		case err := <-consumerErrChan:
			that.log(klog.ErrorLevel, "Start consumer error: %s", err.Error())
			break END_LOOP
		default:
			if msg, ok, isValid := that.queue.TryDequeue(); ok && isValid {
				that.handleMsg(msg, msgHandler)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	remainLen := that.queue.Len()
	// 只有在 Start 成功且收到退出信号时才会走到这里
	that.log(klog.InfoLevel, "Draining remaining %s messages...", remainLen)

	remainMsgs := make([]*Message, remainLen)
	that.queue.DequeueToWait(remainMsgs, 5000*time.Millisecond)

	for _, msg := range remainMsgs {
		msgHandler(that, msg)
	}
	that.log(klog.InfoLevel, "Drain completed.")

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}

}

func (that *PullConsumer) Close() {
	// 1. 抢占关闭权，并通知 Pull Loop 停止
	if !that.draining.CompareAndSwap(false, true) {
		return
	}

	// 2. 取消 Context (解开 SyncSubscribe 的 RUN_LOOP)
	that.ctx.Cancel()

	// 3. 物理退订与停止
	for _, topic := range that.conf.Consumer.Topics {
		// 注意：某些版本的 RocketMQ Go SDK 在 Shutdown 时会自动 Persist，
		// 但为了保险，建议显式调用或确保位点同步。
		_ = that.mqConsumer.PersistOffset(context.Background(), topic)
		_ = that.mqConsumer.Unsubscribe(topic)
	}

	// 4. 关闭通道，触发排水循环 range ch 结束
	that.queue.Close()

	// 5. 停止底层 Client (Shutdown 会等待拉取动作返回)
	err := that.mqConsumer.Shutdown()
	if err != nil {
		that.log(klog.ErrorLevel, "Shutdown consumer error: %s", err.Error())
	}

	// 6. 核心：等待 SyncSubscribe 排水完毕后再返回
	// 这样可以确保主程序在 Close 返回后，数据已经全处理完了
	that.wg.Wait()

	that.ctx.Cancel()
	that.ctx.Remove()
}

//////////////////////////////////////////////////////////////

// 提取公共处理函数
func (that *PullConsumer) handleMsg(msg *Message, callback MessageHandler) {
	if callback != nil {
		callback(that, msg)
		// if that.conf.Consumer.AutoCommit == AUTO_COMMIT_CUSTOM {
		// 	_ = msg.Ack()
		// }
	}
}

// log 日志记录, 会自动添加 RocketLogTag
//
//go:inline
func (that *PullConsumer) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RocketLogTag, 1, format, args...)
	}
}
