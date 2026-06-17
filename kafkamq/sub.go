package kafkamq

import (
	"slices"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/klists"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
)

/////////////////////////////////////////////////////////////

type ConsumerGroup struct {
	ctx        *kcontext.ContextNode
	conf       *Config
	group      sarama.ConsumerGroup
	brokerList []string
	topics     []*Topic                               // 每个 topic 及其偏移量
	queue      *ksync.LockedRingBuffer[*KafkaMessage] // 消息队列
	bufferSize uint                                   // 缓冲区大小
	logf       klog.AppLogFuncWithTag

	wg sync.WaitGroup // 用于同步 Close
}

func NewConsumerGroup(ctx *kcontext.ContextNode, bufferSize uint, conf *Config, logf klog.AppLogFuncWithTag) (*ConsumerGroup, error) {
	config := sarama.NewConfig()
	// 设置config
	config.Version = conf.Version                     // 设置协议版本
	config.ClientID = conf.ClientId                   // 设置客户端 ID
	config.ChannelBufferSize = conf.ChannelBufferSize // 设置通道缓冲区大小

	config.Consumer.MaxProcessingTime = conf.Consumer.MaxProcessingTime // 消费者处理消息的最大时间, 超过后会触发再均衡
	config.Consumer.Fetch.Min = int32(conf.Consumer.Min)                // 每次从broker拉取的最小消息字节数
	config.Consumer.Fetch.Max = int32(conf.Consumer.Max)                // 每次从broker拉取的最大消息字节数
	config.Consumer.Fetch.Default = int32(conf.Consumer.Fetch)          // 默认每次从broker拉取的字节数
	config.Consumer.MaxWaitTime = conf.Consumer.MaxWaitTime             // 拉取消息时最大等待时间, 单位ms, 默认250ms
	config.Consumer.Offsets.Initial = conf.Consumer.InitialOffset       // 设置消费者偏移量, -1: 从最新的消息开始消费, -2: 重新开始消费
	if conf.Consumer.AutoCommit == AUTO_COMMIT_NATIVE {
		config.Consumer.Offsets.AutoCommit.Enable = true                               // 是否自动提交偏移量
		config.Consumer.Offsets.AutoCommit.Interval = conf.Consumer.AutoCommitInterval // 自动提交偏移量的间隔时间
	} else {
		config.Consumer.Offsets.AutoCommit.Enable = false // 禁用自动提交偏移量
	}

	config.Consumer.Return.Errors = conf.Consumer.ReturnError // 是否返回消费过程中遇到的错误, 默认为false

	// 分区分配策略
	assignor := conf.Consumer.GetAssignor()
	if assignor == nil {
		assignor = sarama.NewBalanceStrategyRange() // 默认采用 range 分配策略
	}
	config.Consumer.Group.Rebalance.Strategy = assignor

	config.Consumer.Group.Rebalance.Timeout = conf.Consumer.RebalanceTimeout   // 重分配超时时间
	config.Consumer.Group.Heartbeat.Interval = conf.Consumer.HeartbeatInterval // 心跳间隔时间
	config.Consumer.Group.Session.Timeout = conf.Consumer.SessionTimeout       // 会话过期时间

	// 网络配置
	config.Net.MaxOpenRequests = conf.Net.MaxOpenRequests // 最大请求数, 默认为5，这里设置为1，避免并发请求过多导致Kafka端出现问题
	config.Net.DialTimeout = conf.Net.DialTimeout         // 连接超时时间，默认为30秒
	config.Net.ReadTimeout = conf.Net.ReadTimeout         // 从连接读取消息的超时时间，默认为120秒
	config.Net.WriteTimeout = conf.Net.WriteTimeout       // 向连接写入消息的超时时间，默认为10秒

	// 这一行的作用是: 设置客户端是否在连接 Kafka 时尝试解析 Kafka 集群的主机名称, 默认为 true。
	// 如果设置为 true, 当 Kafka 集群的主机名称为 IP 地址时, 可能会导致连接失败, 因此这里设置为 false。
	config.Net.ResolveCanonicalBootstrapServers = conf.Net.ResolveHost

	// 若通过环境变量提供了 Kerberos 配置，则启用 Kerberos 认证
	if err := applyConsumerKerberosEnv(config); err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "enable Kerberos failed: %s", err.Error())
		}
		return nil, err
	}

	brokerList := klists.ToKSlice(conf.Brokers)
	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "kafka.NewClient error: %s", err.Error())
		}
		return nil, err
	}

	group, err := sarama.NewConsumerGroupFromClient(conf.GroupID, client)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "kafka.NewConsumerGroup error: %s", err.Error())
		}
		return nil, err
	}

	topics := klists.ToKSlice(conf.Topics)
	topics = QueryTopics(client, topics...)

	queue, err := ksync.NewLockedRingBuffer[*KafkaMessage](uint64(bufferSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "Create kafka subscribe queue failed: %s", err.Error())
		}
		return nil, err
	}

	subCtx := ctx.NewChild("kafka_group_consumer")
	return &ConsumerGroup{
		ctx:        subCtx,
		conf:       conf,
		group:      group,
		brokerList: brokerList,
		topics:     topics,
		bufferSize: bufferSize,
		queue:      queue,
		logf:       logf,
	}, nil
}

func (that *ConsumerGroup) Subscribe() {
	go that.SyncSubscribe()
}

// Subscribe 订阅Kafka主题并异步处理消息。从最新偏移量开始收取消息。该函数会阻塞, 直到 Close() 被调用。
//   - @param callback: 当收到新消息时要调用的函数。
func (that *ConsumerGroup) SyncSubscribe() {
	that.wg.Add(1)
	defer that.wg.Done()

	// // 这样可以确保处理协程（subCtx 里的 for 循环）能够被唤醒并退出。
	// defer that.queue.Close() // 无论发生什么，SyncSubscribe 退出时必须关闭队列，

	tmpCtx := that.ctx.NewChild("kafka_group_consumer_tmp")

	topicNameList := make([]string, 0, len(that.topics))
	for _, topic := range that.topics {
		topicNameList = append(topicNameList, topic.Name)
	}

	msgHandler := that.conf.Consumer.MainHandler()
	consumerHandler := func(voidObj any, msg *KafkaMessage) {
		if msgHandler != nil {
			msgHandler(voidObj, msg)
		}
	}

	// 定义消费者组处理程序
	// handler := &privateConsumerGroupHandler{parentCtx: that.ctx, queue: that.queue, topics: that.topics, callback: callback, voidObj: voidObj, bufferSize: that.bufferSize}
	handler := &privateConsumerGroupHandler{parentCtx: that.ctx, queue: that.queue, topics: that.topics, AutoCommit: that.conf.Consumer.AutoCommit, bufferSize: that.bufferSize, logf: that.logf}
	go func(ctx *kcontext.ContextNode, topics []string, handler *privateConsumerGroupHandler) {
		for {
			if err := that.group.Consume(ctx.Context(), topics, handler); err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, KafkaLogTag, "kafka.ConsumerGroup error: %s", err.Error())
				}
			}

			// 检查上下文是否被取消，如果是，函数传入的上下文被取消
			if ctx.Context().Err() != nil {
				break
			}
		}
		if that.logf != nil {
			that.logf(klog.InfoLevel, KafkaLogTag, "kafka.ConsumerGroup done")
		}
	}(tmpCtx, topicNameList, handler)

	var workerWg sync.WaitGroup
	workerWg.Add(1)
	subCtx := that.ctx.NewChild("kafka_group_consumer_child")
	go func(ctx *kcontext.ContextNode) {
		defer workerWg.Done()
		buffer := make([]*KafkaMessage, that.bufferSize)
		for {

			// 使用阻塞式 DequeueTo。
			// 退出逻辑：当 queue.Close() 被调用且数据排干后，n 会返回 0。
			n := that.queue.TryDequeueTo(buffer)
			if n > 0 {
				for _, msg := range buffer[:n] {
					consumerHandler(that, msg)
					// 标记位点。统一由处理协程标记，职责单一，避免 Cleanup 并发竞争。
					if msg.session != nil && that.conf.Consumer.AutoCommit == AUTO_COMMIT_NONE {
						msg.session.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, "")
					}
				}
				continue
			}

			if that.queue.IsClosed() {
				break
			}

			if ctx.Context().Err() != nil {
				break
			}

			// 队列为空但未关闭：让出 CPU，防止 busy loop
			select {
			case <-time.After(2 * time.Millisecond): // 8~15ms 即可，足够灵敏
			case <-ctx.Context().Done():
				continue
			}
		}

		if that.logf != nil {
			that.logf(klog.InfoLevel, KafkaLogTag, "group_consumer_child done")
		}
	}(subCtx)

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	<-that.ctx.Context().Done() // 阻塞等待外部取消信号
	that.group.PauseAll()       // 停止从 Broker 拉取
	tmpCtx.Cancel()             // 此时 tmpCtx 取消会导致 Consume 返回，随后触发 handler.Cleanup

	// 关键：给 Sarama 一点时间把内部消息吐出来
	time.Sleep(ShutdownDrainTimeout * time.Millisecond) // 推荐范围：800ms ~ 2500ms

	// that.group.Close() 会同步阻塞，直到 Sarama 倒完最后一滴水、安全触发 Cleanup、
	// 并且把所有进队的数据位点 100% 正确提交给 Kafka 后，才会返回。
	if err := that.group.Close(); err != nil {
		if that.logf != nil {
			that.logf(klog.ErrorLevel, KafkaLogTag, "Error closing client: %s", err.Error())
		}
	}

	that.queue.Close()
	workerWg.Wait()

	subCtx.Cancel()
	tmpCtx.Remove()
	subCtx.Remove()

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}

	if that.logf != nil {
		that.logf(klog.InfoLevel, KafkaLogTag, "SyncSubscribe done")
	}
}

func (that *ConsumerGroup) Close() {
	if that.ctx == nil {
		return
	}

	that.ctx.Cancel() // 发出停止信号
	that.wg.Wait()    // 等待 SyncSubscribe 跑完所有清理逻辑退出

	if that.logf != nil {
		that.logf(klog.InfoLevel, KafkaLogTag, "ConsumerGroup Close")
	}

	// that.group.Close()
	that.ctx.Remove()
}

/////////////////////////////////////////////////////////////

// ConsumerGroupHandler 实例用于处理单个 topic/partition 的声明。
// 它还提供了用于处理消费者组会话生命周期的钩子，并允许在消费循环(s)之前或之后触发逻辑。
//
// 请注意，处理程序很可能从多个 goroutine 同时并发调用，
// 确保所有状态都安全地防止竞争条件。
type privateConsumerGroupHandler struct {
	parentCtx *kcontext.ContextNode
	queue     *ksync.LockedRingBuffer[*KafkaMessage] // 消息队列
	topics    []*Topic

	// callback   MessageHandler
	// voidObj    any
	bufferSize uint
	AutoCommit string
	once       sync.Once
	logf       klog.AppLogFuncWithTag
}

// Setup 是在新的会话开始之前调用的，在 ConsumeClaim 之前。
func (that *privateConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	that.once.Do(func() {
		// 1. 获取当前消费者被分配到的所有 Topic 和 Partition 映射
		claims := session.Claims()
		for topicName, partitions := range claims {
			for _, topic := range that.topics {
				// 2. 只有在当前分配到的 Topic 中寻找匹配项
				if topic.Name != topicName {
					continue
				}

				for partition, offset := range topic.Partition {
					if !slices.Contains(partitions, partition) {
						continue
					}

					if offset >= 0 {
						session.ResetOffset(topic.Name, partition, offset, "")
						// session.MarkOffset(topic.Name, partition, offset, "")
					}
				}
			}
		}
	})
	return nil
}

// Cleanup 是在会话结束之前运行的，在所有 ConsumeClaim 协程退出之后，
// 但在最后一次提交偏移量之前。
func (that *privateConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	if that.logf != nil {
		that.logf(klog.InfoLevel, KafkaGroupHandlerLogTag, "ConsumerGroupHandler cleanup end")
	}
	isClosing := that.isParentCtxDone() // 检查是否是用户主动关闭了 context
	if isClosing {
		// // 无论是否是主动关闭，Cleanup 的唯一核心职责是“发信号”
		// // 只有在主动关闭时才调用 Close() 破坏阻塞，触发处理协程排干数据并退出
		// that.queue.Close()
		if that.logf != nil {
			that.logf(klog.InfoLevel, KafkaGroupHandlerLogTag, "ConsumerGroupHandler cleanup done")
		}
	}

	return nil
}

// ConsumeClaim 必须启动 ConsumerGroupClaim 的 Messages() 消费循环。
// 一旦 Messages() 通道被关闭，Handler 必须完成其消息处理循环并退出。
func (that *privateConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		kafkMsg := &KafkaMessage{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset, Key: msg.Key, Value: msg.Value, session: session}

		enqueued := false
		for {
			// 1. 每次循环优先检查上下文。
			//    如果主协程触发了 tmpCtx.Cancel()，或者 Sarama 自身的 session 结束了：
			//    立刻无条件 break 退出自旋。
			if session.Context().Err() != nil || that.parentCtx.Context().Err() != nil {
				break
			}

			// 2. 尝试非阻塞入队。如果成功，标记并跳出当前消息的自旋
			if ok := that.queue.TryEnqueue(kafkMsg); ok {
				enqueued = true
				break
			}

			// 走到这里说明队列满了。为了不让 CPU 空转（避免暴涨到 100%），让出时间片，歇 1 毫秒后继续探测。
			// 1ms 的响应速度在工业级退出场景下已经是微秒级的灵敏度了。
			time.Sleep(1 * time.Millisecond)
		}

		// 如果是因为 Context 取消导致最终没能成功入队：我们直接 return nil 或者 break，结束当前分区的消费循环
		if !enqueued {
			break
		}

		// 只有真正成功入队的数据，才在符合条件时触发手动标记/提交
		// （注意：如果你用的是 AUTO_COMMIT_NATIVE，下面这段原本就不用写，留空即可）
		if that.AutoCommit == AUTO_COMMIT_CUSTOM {
			session.MarkMessage(msg, "")
			if msg.Offset%500 == 0 {
				session.Commit()
			}
		}
	}
	return nil
}

// 辅助函数：判断外部大的上下文是否已经取消
func (that *privateConsumerGroupHandler) isParentCtxDone() bool {
	select {
	case <-that.parentCtx.Context().Done():
		return true
	default:
		return false
	}
}
