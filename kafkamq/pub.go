package kafkamq

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/klists"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
)

type AsyncProducer struct {
	ctx        *kcontext.ContextNode
	brokerList []string
	conf       *Config
	Producer   sarama.AsyncProducer
	queue      *ksync.LockedRingBuffer[*KafkaMessage] // 消息队列
	queueSize  uint                                   // 消息通道大小
	logf       klog.AppLogFuncWithTag

	wg sync.WaitGroup // 用于同步 Close
}

// NewAsyncProducer 创建一个新的异步 Kafka 生产者。
//
// 参数:
//
//	ctx：生产者的上下文。
//	brokerList：连接到 Kafka 的 broker 列表。
//	logf：用于记录错误的日志函数。
//
// 返回:
//
//	*AsyncProducer：创建的异步生产者的指针。
func NewAsyncProducer(ctx *kcontext.ContextNode, queueSize uint, conf *Config, logf klog.AppLogFuncWithTag) (*AsyncProducer, error) {
	config := sarama.NewConfig()
	// 设置config
	config.Version = conf.Version                     // 设置协议版本
	config.ClientID = conf.ClientId                   // 设置客户端 ID
	config.ChannelBufferSize = conf.ChannelBufferSize // 设置通道缓冲区大小

	// 网络配置
	config.Net.MaxOpenRequests = conf.Net.MaxOpenRequests // 最大请求数, 默认为5，应避免并发请求过多导致Kafka端出现问题
	config.Net.DialTimeout = conf.Net.DialTimeout         // 连接超时时间，默认为30秒
	config.Net.ReadTimeout = conf.Net.ReadTimeout         // 从连接读取消息的超时时间，默认为120秒
	config.Net.WriteTimeout = conf.Net.WriteTimeout       // 向连接写入消息的超时时间，默认为10秒

	// 这一行的作用是: 设置客户端是否在连接 Kafka 时尝试解析 Kafka 集群的主机名称, 默认为 false
	// 如果设置为 true, 当 Kafka 集群的主机名称为 IP 地址时, 可能会导致连接失败, 因此这里设置为 false。
	config.Net.ResolveCanonicalBootstrapServers = conf.Net.ResolveHost

	// 设置 Producer 的配置
	config.Producer.Compression = conf.Producer.GetCompression()       // 设置压缩方式: snappy
	config.Producer.CompressionLevel = conf.Producer.CompressionLevel  // 设置压缩级别
	config.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes    // 发送限制
	config.Producer.RequiredAcks = conf.Producer.GetRequiredAcks()     // 设置确认模式: 本地文件写入成功，并不代表已经通知服务器
	config.Producer.Idempotent = conf.Producer.Idempotent              // 是否开启幂等性, 默认为false
	config.Producer.Return.Errors = conf.Producer.ReturnError          // 是否返回消费过程中遇到的错误, 默认为false
	config.Producer.Return.Successes = conf.Producer.ReturnAck         // 是否返回消费完成, 默认为false
	config.Producer.Flush.Messages = conf.Producer.FlushMessages       // 设置刷新消息数量: 每100条刷新
	config.Producer.Flush.Frequency = conf.Producer.FlushFrequency     // 缓存时间
	config.Producer.Flush.MaxMessages = conf.Producer.FlushMaxMessages // 设置刷新最大消息数量: 10000条
	config.Producer.Retry.Max = conf.Producer.RetryMax                 // 设置重试次数: 最多重试3次
	config.Producer.Timeout = conf.Producer.Timeout                    // 设置发送超时时间: 30s

	// 若通过环境变量提供了 Kerberos 配置，则启用 Kerberos 认证
	if err := applyProducerKerberosEnv(config); err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "enable Kerberos failed: %s", err.Error())
		}
		return nil, err
	}

	brokerList := klists.ToKSlice(conf.Brokers)
	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "kafka.NewAsyncProducer error: %s", err.Error())
		}
		return nil, err
	}

	queue, err := ksync.NewLockedRingBuffer[*KafkaMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, KafkaLogTag, "Create kafka publish queue failed: %s", err.Error())
		}
		return nil, err
	}

	subCtx := ctx.NewChild("kafka_async_producer")
	producerClient := &AsyncProducer{
		ctx:        subCtx,
		brokerList: brokerList,
		conf:       conf,
		Producer:   producer,
		queue:      queue,
		queueSize:  queueSize,
		logf:       logf,
	}
	return producerClient, nil
}

func (that *AsyncProducer) Start() {
	that.wg.Add(1)
	defer that.wg.Done()

	var workerWg sync.WaitGroup // 使用局部 WaitGroup 控制子协程（搬运工）
	workerWg.Add(1)
	subCtx, subCancel := context.WithCancel(context.Background())
	go func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		successesChan := that.Producer.Successes() // 成功状态通道
		errorsChan := that.Producer.Errors()       // 失败状态通道

	END_LOOP:
		for {
			select {
			case <-ctx.Done():
				break END_LOOP
			case success, ok := <-successesChan:
				if !ok { // Producer 关闭后通道关闭，退出
					return
				}
				if KafkaTraceFlag {
					byteArr, _ := success.Value.Encode() // 比较耗时, 调试期间才需要
					that.log(klog.DebugLevel, "Message sent to Kafka topic %s, partition %d, offset %d msg: %s", success.Topic, success.Partition, success.Offset, string(byteArr))
				}
			case err, ok := <-errorsChan:
				if !ok {
					return
				}
				that.log(klog.ErrorLevel, "Failed to send message to Kafka topic %s, partition %d: %s", err.Msg.Topic, err.Msg.Partition, err.Err.Error())
				if that.conf.OnError != nil {
					that.conf.OnError(err)
				}
			}
		}

		that.log(klog.InfoLevel, "kafka producer status channel done")
	}(subCtx, &workerWg)

	workerWg.Add(1)
	go func(cancelFunc context.CancelFunc, wg *sync.WaitGroup) {
		defer wg.Done()

		buffer := make([]*KafkaMessage, that.queueSize)
		for {
			// 退出逻辑：当 queue.Close() 被调用且数据排空后，n 会返回 0。
			n := that.queue.DequeueTo(buffer)
			if n <= 0 {
				break // 实现排水：余粮出尽，关门谢客
			}
			for _, msg := range buffer[:n] {
				rawMsg := &sarama.ProducerMessage{Topic: msg.Topic, Key: sarama.StringEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value), Headers: msg.Headers}
				// 注意：在 Close() 调用后，只要没有调用 Producer.Close()，Input 依然可用
				that.Producer.Input() <- rawMsg
			}
		}
		subCancel()

		that.log(klog.InfoLevel, "kafka producer send goroutine done")
	}(subCancel, &workerWg)

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	<-that.ctx.Context().Done() // 阻塞等待外部取消信号（如调用了 Close 方法）
	workerWg.Wait()

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}

	that.log(klog.InfoLevel, "kafka producer runloop done")
}

func (that *AsyncProducer) Publish(msg *KafkaMessage) bool {
	if that != nil && that.queue != nil {
		return that.queue.Enqueue(msg)
	}
	return false
}

func (that *AsyncProducer) PublishArray(msgs []*KafkaMessage) bool {
	if that != nil && that.queue != nil {
		n := that.queue.EnqueueBatch(msgs)
		return n > 0
	}
	return false
}

func (that *AsyncProducer) PublisMessage(topic, key, message string) bool {
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Key:       []byte(key),
		Value:     []byte(message),
	}
	return that.Publish(msg)
}

func (that *AsyncProducer) PublishData(partition int32, topic, key string, value []byte) bool {
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    0,
		Key:       []byte(key),
		Value:     value,
	}
	return that.Publish(msg)
}

func (that *AsyncProducer) PublishDataWithProperties(partition int32, topic, key string, value []byte, properties map[string]string) bool {
	headers := make([]sarama.RecordHeader, 0, len(properties))
	for k, v := range properties {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    0,
		Key:       []byte(key),
		Value:     value,
		Headers:   headers,
	}
	return that.Publish(msg)
}

func (that *AsyncProducer) Close() {
	if that == nil || that.ctx == nil {
		return
	}
	that.ctx.Cancel()
	that.queue.Close() // 开始排水流程, 操作会唤醒阻塞在 DequeueTo 的 daemonCtx 协程，并让它一次性取出余下所有消息。
	that.wg.Wait()     // 等待 Start 内部所有 wg 任务完成

	// that.Producer.AsyncClose()

	// 先关闭 Producer，利用其阻塞特性等待数据 Flush 完毕
	if that.Producer != nil {
		if err := that.Producer.Close(); err != nil {
			that.log(klog.ErrorLevel, "Error closing sarama producer: %s", err.Error())
		}
	}

	that.ctx.Remove()
}

// log 日志记录, 会自动添加 KafkaLogTag
//
//go:inline
func (that *AsyncProducer) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, KafkaLogTag, format, args...)
	}
}
