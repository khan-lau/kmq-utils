package natsmq

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/expr/condexpr"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
	"github.com/nats-io/nats.go"
)

type NatsJetStreamClient struct {
	ctx  *kcontext.ContextNode
	conf *NatsClientConfig

	conn   *nats.Conn
	js     nats.JetStreamContext         // JetStream 上下文
	stream *nats.StreamInfo              // Stream 信息
	subs   map[string]*nats.Subscription // 持有的订阅对象

	// 状态管理
	connected atomic.Bool
	draining  atomic.Bool
	started   atomic.Bool
	readyOnce sync.Once
	wg        sync.WaitGroup

	timestamp int64 // 上次消费纳秒时间戳，用于断点续传

	// 发送队列 (与 CoreClient 对齐)
	queue     *ksync.LockedRingBuffer[*NatsMessage]
	queueSize uint

	logf klog.AppLogFuncWithTag
}

func NewNatsJetStreamClient(ctx *kcontext.ContextNode, queueSize uint, timestamp int64, conf *NatsClientConfig, logf klog.AppLogFuncWithTag) (*NatsJetStreamClient, error) {
	if conf.JetStream() == nil {
		return nil, fmt.Errorf("NatsClientConfig.JetStream is nil")
	}

	queue, err := ksync.NewLockedRingBuffer[*NatsMessage](uint64(queueSize))
	if err != nil {
		return nil, err
	}

	subCtx := ctx.NewChild("natsmq_js_pubsub")
	return &NatsJetStreamClient{
		ctx:       subCtx,
		conf:      conf,
		conn:      nil,
		js:        nil,
		stream:    nil,
		subs:      make(map[string]*nats.Subscription),
		connected: atomic.Bool{},
		draining:  atomic.Bool{},
		started:   atomic.Bool{},
		readyOnce: sync.Once{},
		wg:        sync.WaitGroup{},
		timestamp: timestamp,
		queue:     queue,
		queueSize: queueSize,
		logf:      logf,
	}, nil
}

// Start 同步启动：连接、配置 Stream、对齐订阅
func (that *NatsJetStreamClient) Start() error {
	if !that.started.CompareAndSwap(false, true) {
		return nil
	}

	// 1. 基础连接 + 获取 JS 上下文
	if err := that.doConnect(); err != nil {
		that.started.Store(false)
		return err
	}

	// 确保物理连接成功后立即同步状态，防止 startDrainPipe 启动瞬间误判
	if that.conn != nil && that.conn.IsConnected() {
		that.connected.Store(true)
	}

	// 2. 自动同步订阅 (Push 模式)
	if err := that.syncSubscriptions(); err != nil {
		that.conn.Close()
		that.started.Store(false)
		return err
	}

	// 3. 准备就绪
	that.readyOnce.Do(func() {
		if that.conf != nil && that.conf.OnReady != nil {
			that.conf.OnReady(true)
		}
	})

	// 4. 开启异步发送管道
	that.startDrainPipe()

	return nil
}

// Close 优雅停机
func (that *NatsJetStreamClient) Close() {
	if that.draining.Swap(true) {
		return
	}

	// 1. 订阅 Drain (JetStream 会处理 Ack 的存量)
	for _, sub := range that.subs {
		_ = sub.Drain()
	}

	// 2. 队列封口，解开 DequeueTo 阻塞
	that.queue.Close()

	// 3. 等待管道清空
	done := make(chan struct{})
	go func() {
		that.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		that.log(klog.InfoLevel, "NatsJetStreamClient buffer drained successfully")
	case <-time.After(20 * time.Second):
		that.log(klog.WarnLevel, "NatsJetStreamClient drain timeout, forcing shutdown")
	}

	// 4. 断开 NATS 连接
	if that.conn != nil {
		_ = that.conn.Drain()
		that.conn.Close()
	}

	that.ctx.Cancel() // 立即通知后台退出重试
	that.ctx.Remove()
}

func (that *NatsJetStreamClient) PublishMessage(topic string, message string) bool {
	msg := &NatsMessage{
		Topic:   topic,
		Payload: []byte(message),
	}
	return that.Publish(msg)
}

// Publish 业务层异步发送入口
func (that *NatsJetStreamClient) Publish(msg *NatsMessage) bool {
	if that.draining.Load() || that.conn == nil {
		return false
	}
	return that.queue.Enqueue(msg)
}

///////////////////////////////////////////////////////////////

func (that *NatsJetStreamClient) doConnect() error {
	opts := make([]nats.Option, 0, 30)
	opts = append(opts, nats.Name(that.conf.Nats().name)) // 设置客户端名称
	if len(that.conf.Nats().User()) > 0 && len(that.conf.Nats().Password()) > 0 {
		opts = append(opts, nats.UserInfo(that.conf.Nats().User(), that.conf.Nats().Password())) // 用户名密码模式
	} else if len(that.conf.Nats().User()) == 0 && len(that.conf.Nats().Password()) > 0 {
		opts = append(opts, nats.Token(that.conf.nats.Password())) // token模式
	}

	// 允许TLS连接
	if that.conf.Nats().UseTls() {
		// 加载客户端证书和密钥
		cert, err := tls.LoadX509KeyPair(that.conf.Nats().TlsClientCert(), that.conf.Nats().KeyPath())
		if err != nil {
			that.log(klog.WarnLevel, "Error parsing X509 certificate/key pair: %v", err)
			return err
		}

		// 初始化 TLS 配置
		tlsConfig := &tls.Config{
			ServerName:   "", // 服务器名称，用于证书验证等用途, 此处无需设置, 由nats.go中内部自动设置, 具体见 `func (nc *Conn) makeTLSConn() error`
			Certificates: []tls.Certificate{cert},
			MinVersion:   uint16(that.conf.Nats().MinTlsVer()),
		}

		var systemCertPool *x509.CertPool
		if that.conf.Nats().insecureSkipVerify { // 允许无CA
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.RootCAs = nil
		} else {
			// 获取证书池
			systemCertPool, err = x509.SystemCertPool()
			if err != nil {
				that.log(klog.WarnLevel, "Warning: Could not load system root CA pool: %v. Using empty pool.", err)
				systemCertPool = x509.NewCertPool()
			}
			tlsConfig.RootCAs = systemCertPool
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	if that.conf.Nats().AllowReconnect() {
		opts = append(opts, nats.MaxReconnects(that.conf.Nats().MaxReconnect()))
		opts = append(opts, nats.ReconnectWait(time.Duration(that.conf.Nats().ReconnectWait())))

		opts = append(opts, nats.ReconnectBufSize(that.conf.Nats().ReconnectBufSize())) // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息
	}

	opts = append(opts, nats.PingInterval(time.Duration(that.conf.Nats().PingInterval()))) // 设置ping间隔时间
	opts = append(opts, nats.MaxPingsOutstanding(that.conf.Nats().MaxPingsOut()))          // 设置最大允许的ping无应答次数
	opts = append(opts, nats.DrainTimeout(15000*time.Millisecond))                         // 设置 Drain(排水) 超时时长, 与Close的20s超时对应, 必须小于该时长

	// 连接成功时调用
	opts = append(opts, nats.ConnectHandler(func(nc *nats.Conn) {
		that.connected.Store(true)
	}))

	// 连接断开时调用
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		that.connected.Store(false)
	}))

	// Connect to a server
	conn, err := nats.Connect(strings.Join(that.conf.nats.Servers(), ","), opts...) // 连接NATS服务器, 允许同时连接多个服务器地址, 逗号分隔
	if err != nil {
		// that.log(klog.WarnLevel, "Connect error: %v", err)
		if that.conf.OnError() != nil {
			that.conf.OnError()(err)
		}
		return err
	}

	that.conn = conn

	js, err := that.conn.JetStream()
	if err != nil {
		// that.log(klog.WarnLevel, "JetStream error: %v", err)
		return err
	}
	that.js = js
	stream, err := that.upsertJetstream(js)
	if err != nil {
		// that.log(klog.WarnLevel, "upsert JetStream error: %v", err)
		return err
	}

	that.stream = stream
	return nil
}

func (that *NatsJetStreamClient) startDrainPipe() {
	that.wg.Add(1)

	go func() {
		defer that.wg.Done()

		buffer := make([]*NatsMessage, that.queueSize)
		for {
			n := that.queue.DequeueTo(buffer)
			if n <= 0 {
				return
			}

			for _, msg := range buffer[:n] {
				for {
					// 1. 如果环境已取消（Context Done），说明强制关机，不再重试
					if that.ctx.Context().Err() != nil {
						return
					}

					// 2. 检查链路
					if that.connected.Load() {
						// JS 同步发布，利用重试逻辑保证送达
						err := that.publishData(msg, "")
						if err == nil {
							break // 发送成功
						}

						// 如果在关机过程中收到连接关闭或正在排水的错误, 此时 NATS 不再接受新数据，重试无意义，立即退出以加速关机
						if that.draining.Load() && (err == nats.ErrConnectionClosed || err == nats.ErrConnectionDraining) {
							return
						}
					}

					// 3. 【核心修正】排水模式下的抉择
					if that.draining.Load() {
						// 如果处于排水模式且网络断了
						// 我们依赖 Close() 中的 select timeout (例如10秒) 来兜底
						// 这里不应该直接 return，而是继续 Sleep 尝试，直到网络恢复或被外部强杀
						if !that.connected.Load() {
							// 如果你希望“死等”，就继续 Sleep
							// 如果你希望“超时放弃”，可以通过检查一个时间戳来决定是否 return
							that.log(klog.WarnLevel, "Drain failed due to disconnected link, dropping remaining messages.")
							return
						}
					}

					// 失败退避
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}()
}

func (that *NatsJetStreamClient) syncSubscriptions() error {
	jsConf := that.conf.JetStream()
	topics := jsConf.Topics()
	if len(topics) == 0 {
		return nil
	}

	// 预创建或更新 Consumer
	if that.conf.JetStream().Consumer() != nil {
		if _, err := that.upsertConsumer(that.js); err != nil {
			return err
		}
	}

	// 准备消息处理Handler
	handler := jsConf.MainHandler()
	natsHandler := func(msg *nats.Msg) {
		if handler != nil {
			meta, err := msg.Metadata()
			if err != nil {
				that.log(klog.WarnLevel, "Failed to get message metadata: %v", err)
				return
			}
			handler(that, &NatsMessage{Seq: meta.Timestamp.UnixNano(), Topic: msg.Subject, Reply: msg.Reply, Header: msg.Header, Payload: msg.Data, origin: msg})
		}

		// if that.conf.jetStream.consumer != nil && (that.conf.jetStream.consumer.AutoCommit() == AUTO_COMMIT_NATIVE || that.conf.jetStream.consumer.AutoCommit() == AUTO_COMMIT_CUSTOM) {
		if that.conf.jetStream.consumer != nil && (that.conf.jetStream.consumer.AutoCommit() == AUTO_COMMIT_NATIVE) {
			err := msg.Ack()
			if err != nil {
				that.log(klog.WarnLevel, "Failed to ack topic: %s, message: %v", msg.Subject, err)
			}
		}
	}

	for _, topic := range topics {
		var sub *nats.Subscription
		var err error

		if that.conf.JetStream().Consumer() != nil && len(that.conf.JetStream().Consumer().Name()) > 0 {
			// 群组订阅
			sub, err = that.js.QueueSubscribe(topic, that.conf.JetStream().Consumer().Name(), natsHandler, nats.Durable(that.conf.JetStream().Consumer().Name()))
		} else {
			// 普通订阅
			sub, err = that.js.Subscribe(topic, natsHandler)
		}

		if err != nil {
			that.log(klog.WarnLevel, "%s error: %v", (condexpr.CondExpr(len(that.conf.JetStream().Consumer().Name()) > 0, "QueueSubscribe", "Subscribe")), err)
			return err
		}

		that.subs[topic] = sub
	}

	return nil
}

// 更新或创建stream
func (that *NatsJetStreamClient) upsertJetstream(js nats.JetStreamContext) (*nats.StreamInfo, error) {
	topics := that.conf.JetStream().Topics()

	jsCfg := &nats.StreamConfig{
		Name:              that.conf.JetStream().Name(),
		Subjects:          topics,
		Storage:           that.conf.JetStream().StorageType(),
		Compression:       that.conf.JetStream().Compression(),
		Retention:         that.conf.JetStream().RetentionPolicy(),
		MaxConsumers:      that.conf.JetStream().MaxConsumers(),
		MaxMsgs:           that.conf.JetStream().MaxMsgs(),
		MaxBytes:          that.conf.JetStream().MaxBytes(),
		MaxAge:            time.Duration(that.conf.JetStream().MaxAge()),
		MaxMsgsPerSubject: that.conf.JetStream().MaxMsgsPerSubject(),
		MaxMsgSize:        that.conf.JetStream().MaxMsgSize(),
		Duplicates:        time.Duration(that.conf.JetStream().Duplicates()),
		Discard:           that.conf.JetStream().Discard(),
		// NoAck:             that.conf.JetStream().NoAck(), // capturing all subjects requires no-ack to be true
	}

	// 创建jetstream
	stream, err := js.AddStream(jsCfg)
	if err != nil {
		if !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			// that.log(klog.WarnLevel, "AddStream error: %v", err)
			return nil, err
		} else {
			// 如果jetstream存在, 则更新配置
			stream, err = js.UpdateStream(jsCfg)
			if err != nil {
				if strings.Contains(err.Error(), "stream configuration update can not change MaxConsumers") {
					that.log(klog.WarnLevel, "Ignoring MaxConsumers update error: %v", err)
				} else {
					// that.log(klog.WarnLevel, "AddStream error: %v", err)
					// return nil, err
				}

			}
		}
	}
	return stream, nil
}

// 更新或创建consumer
func (that *NatsJetStreamClient) upsertConsumer(js nats.JetStreamContext) (*nats.ConsumerInfo, error) {
	if that.conf.JetStream().Consumer() == nil {
		return nil, nil
	}

	filterSubjects := that.conf.JetStream().Topics()
	consumerCfg := &nats.ConsumerConfig{
		Durable:        that.conf.JetStream().Consumer().Name(),
		Name:           that.conf.JetStream().Consumer().Name(),
		FilterSubjects: filterSubjects,
		// MaxWaiting:     int(that.conf.JetStream().Consumer().MaxWait()), // pull模式专用参数
		AckPolicy:      that.conf.JetStream().Consumer().AckPolicy(),
		DeliverPolicy:  that.conf.JetStream().Consumer().DeliverPolicy(),
		DeliverSubject: fmt.Sprintf("_INBOX.%v", that.conf.JetStream().Consumer().Name()), // Push 模式, 推送模式时, 需要指定一个临时主题, 用于接收消息
		DeliverGroup:   that.conf.JetStream().Consumer().Name(),
		MaxAckPending:  1000,
	}

	if that.timestamp >= 0 {
		startTime := time.Unix(0, that.timestamp)
		consumerCfg.OptStartTime = &startTime                     // 断点续传
		consumerCfg.DeliverPolicy = nats.DeliverByStartTimePolicy // 从指定时间开始消费
	} else {
		consumerCfg.OptStartTime = nil
	}

	consumer, err := js.ConsumerInfo(that.conf.JetStream().Name(), that.conf.JetStream().Consumer().Name())
	if err != nil {
		if errors.Is(err, nats.ErrConsumerNotFound) {
			consumer, err = js.AddConsumer(that.conf.JetStream().Name(), consumerCfg)
			if err != nil {
				that.log(klog.WarnLevel, "Create Consumer error: %v", err)
				return nil, err
			}
		} else {
			that.log(klog.WarnLevel, "Get Consumer error: %v", err)
			return nil, err
		}
	}

	return consumer, nil
}

func (that *NatsJetStreamClient) publishData(msg *NatsMessage, msgId string) error {
	msgWithKey := nats.NewMsg(msg.Topic)
	msgWithKey.Reply = msg.Reply
	msgWithKey.Data = msg.Payload
	msgWithKey.Header = msg.Header
	pubOpts := make([]nats.PubOpt, 0, 1)
	if len(msgId) > 0 {
		pubOpts = append(pubOpts, nats.MsgId(msgId))
	}

	// 同步发送消息
	ack, err := that.js.PublishMsg(msgWithKey, pubOpts...)
	if err != nil {
		return err
	} else {
		// id重复, 被服务器忽略了, 不会投递到订阅者中
		if ack.Duplicate {
			that.log(klog.WarnLevel, "Publish duplicate: %v", ack)
		}
	}
	return nil
}

// log 日志记录, 会自动添加 NatsLogTag
//
//go:inline
func (that *NatsJetStreamClient) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, NatsLogTag, 1, format, args...)
	}
}
