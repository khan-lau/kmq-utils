package natsmq

import (
	"crypto/tls"
	"crypto/x509"
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

type NatsCoreClient struct {
	ctx  *kcontext.ContextNode
	conf *NatsClientConfig
	conn *nats.Conn

	// 状态管理
	connected atomic.Bool
	draining  atomic.Bool
	started   atomic.Bool
	readyOnce sync.Once
	wg        sync.WaitGroup

	subs map[string]*nats.Subscription // 订阅列表, key为主题名称, value为订阅对象

	// 发送队列 (改用之前的 LockedRingBuffer 性能更好)
	queue     *ksync.LockedRingBuffer[*NatsMessage]
	queueSize uint

	logf klog.AppLogFuncWithTag
}

func NewNatsCoreClient(ctx *kcontext.ContextNode, queueSize uint, conf *NatsClientConfig, logf klog.AppLogFuncWithTag) (*NatsCoreClient, error) {
	if conf.CoreNats() == nil {
		return nil, fmt.Errorf("NatsClientConfig.CoreNats is nil")
	}

	queue, err := ksync.NewLockedRingBuffer[*NatsMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, NatsLogTag, "Create natsmq queue failed: %s", err.Error())
		}
		return nil, err
	}

	subCtx := ctx.NewChild("natsmq_pubsub")
	coreClient := &NatsCoreClient{
		ctx:       subCtx,
		conf:      conf,
		conn:      nil,
		subs:      make(map[string]*nats.Subscription),
		connected: atomic.Bool{},
		draining:  atomic.Bool{},
		started:   atomic.Bool{},
		readyOnce: sync.Once{},
		wg:        sync.WaitGroup{},
		queue:     queue,
		queueSize: queueSize,
		logf:      logf,
	}
	return coreClient, nil
}

// Start 同步启动：阻塞直到连接成功并完成订阅
func (that *NatsCoreClient) Start() error {
	if !that.started.CompareAndSwap(false, true) {
		return nil
	}

	// 1. 执行连接
	if err := that.doConnect(); err != nil {
		that.started.Store(false)
		return err
	}

	// 确保物理连接成功后立即同步状态，防止 startDrainPipe 启动瞬间误判
	if that.conn != nil && that.conn.IsConnected() {
		that.connected.Store(true)
	}

	// 2. 执行订阅 (根据配置自动对齐主题)
	if err := that.syncSubscriptions(); err != nil {
		that.conn.Close()
		that.started.Store(false)
		return err
	}

	// 3. 触发 Ready 回调
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
func (that *NatsCoreClient) Close() {
	if that.draining.Swap(true) {
		return
	}
	// 1. 【止血】NATS 原生支持 Subscription Drain
	for _, sub := range that.subs {
		_ = sub.Drain() // 停止接收新消息，处理完存量后关闭订阅
	}

	// 2. 【封口】
	that.queue.Close()

	// 3. 【排水】等待后台发送协程
	done := make(chan struct{})
	go func() {
		that.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		that.log(klog.InfoLevel, "NatsCoreClient buffer drained successfully")
	case <-time.After(20 * time.Second):
		that.log(klog.WarnLevel, "NatsCoreClient drain timeout, forcing shutdown")
	}

	// 4. 【断开】物理连接 Drain
	if that.conn != nil {
		_ = that.conn.Drain() // 确保 Publish 缓冲区刷新到 Server
		that.conn.Close()     // 显式关闭，确保所有底层 socket 释放
	}

	that.ctx.Cancel()
	that.ctx.Remove()
}

func (that *NatsCoreClient) PublishMessage(topic string, message string) bool {
	msg := &NatsMessage{
		Topic:   topic,
		Payload: []byte(message),
	}
	return that.Publish(msg)
}

// Publish 业务层异步发送入口
func (that *NatsCoreClient) Publish(msg *NatsMessage) bool {
	if that.draining.Load() || that.conn == nil {
		return false
	}
	return that.queue.Enqueue(msg)
}

///////////////////////////////////////////////////////////////

func (that *NatsCoreClient) doConnect() error {
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
		that.log(klog.WarnLevel, "Connect error: %v", err)

		if that.conf.OnError() != nil {
			that.conf.OnError()(err)
		}
		return err
	}

	that.conn = conn
	return nil
}

// startDrainPipe 异步发送循环
func (that *NatsCoreClient) startDrainPipe() {
	that.wg.Add(1)

	go func() {
		defer that.wg.Done()

		buffer := make([]*NatsMessage, that.queueSize)
		for {
			n := that.queue.DequeueTo(buffer)
			if n <= 0 {
				break
			}

			for _, msg := range buffer[:n] {
				for {
					// 1. 如果环境已取消（Context Done），说明强制关机，不再重试
					if that.ctx.Context().Err() != nil {
						return
					}

					// 2. 检查链路
					if that.connected.Load() {
						// 同步发布，利用重试逻辑保证送达
						err := that.conn.Publish(msg.Topic, msg.Payload)
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

// syncSubscriptions 内部方法：对齐订阅视图
func (that *NatsCoreClient) syncSubscriptions() error {
	coreConf := that.conf.CoreNats()
	topics := coreConf.Topics()

	// 没有订阅主题 则直接返回
	if len(topics) == 0 {
		return nil
	}

	handler := coreConf.MainHandler() // 假设配置中有此 handler
	natsHandler := func(msg *nats.Msg) {
		if handler != nil {
			handler(that, &NatsMessage{Topic: msg.Subject, Reply: msg.Reply, Header: msg.Header, Payload: msg.Data})
		}
	}

	for _, topic := range topics {
		var sub *nats.Subscription
		var err error

		// 异步模式
		if len(that.conf.CoreNats().QueueGroup()) > 0 { // 非空时为负载均衡模式, core模式下 没有 类似kafka的通过key的hash实现的负载均衡模式
			sub, err = that.conn.QueueSubscribe(topic, that.conf.CoreNats().QueueGroup(), natsHandler) // 异步模式, 负载均衡模式
		} else {
			sub, err = that.conn.Subscribe(topic, natsHandler) // 异步模式, 非负载均衡模式
		}

		if err != nil {
			that.log(klog.WarnLevel, "%s topic: %s, error: %v", (condexpr.CondExpr(len(that.conf.CoreNats().QueueGroup()) > 0, "QueueSubscribe", "Subscribe")), topic, err)
		} else {
			if err := sub.SetPendingLimits(that.conf.CoreNats().maxPending, -1); err == nil { // 设置消息队列大小限制, 字节数无限制
				that.subs[topic] = sub
			} else {
				that.log(klog.WarnLevel, "SetPendingLimits error: %v", err)
			}
		}
	}

	return nil
}

// log 日志记录, 会自动添加 NatsLogTag
//
//go:inline
func (that *NatsCoreClient) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, NatsLogTag, format, args...)
	}
}
