package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/expr/condexpr"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
)

type MqttClient struct {
	ctx    *kcontext.ContextNode
	conf   *Config
	client paho.Client

	// 状态管理
	connected atomic.Bool    // 物理链路连接状态
	draining  atomic.Bool    // 是否正在执行退出流程
	started   atomic.Bool    // 保证 Start 幂等
	readyOnce sync.Once      // 保证 OnReady 仅触发一次
	wg        sync.WaitGroup // ksync.CountDownLatch

	queue     *ksync.LockedRingBuffer[*MqttMessage] // 消息通道，用于接收订阅的消息
	queueSize uint                                  // 消息通道大小
	logf      klog.AppLogFuncWithTag
}

func NewMqttClient(ctx *kcontext.ContextNode, queueSize uint, conf *Config, logf klog.AppLogFuncWithTag) (*MqttClient, error) {
	if conf.UseTLS() && !filesystem.IsFileExists(conf.CaCertPath()) {
		return nil, fmt.Errorf("mqtt tls ca cert path: %s not exists", conf.CaCertPath())
	}

	queue, err := ksync.NewLockedRingBuffer[*MqttMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, mqtt_tag, "Create mqtt queue failed: %s", err.Error())
		}
		return nil, err
	}

	return &MqttClient{
		ctx:       ctx,
		conf:      conf,
		client:    nil,
		connected: atomic.Bool{},
		draining:  atomic.Bool{},
		started:   atomic.Bool{},
		readyOnce: sync.Once{}, // 保证 OnReady 仅触发一次
		wg:        sync.WaitGroup{},
		queue:     queue,
		queueSize: queueSize,
		logf:      logf,
	}, nil
}

// Start 同步接口：阻塞直到连接建立并下发初始订阅。
// 返回 nil 即代表 Ready。
func (that *MqttClient) Start() error {
	if !that.started.CompareAndSwap(false, true) {
		return nil
	}

	opts := paho.NewClientOptions()
	scheme := condexpr.CondExpr(that.conf.useTLS, "ssl://", "tcp://") // 动态设置 Broker URL
	brokerURL := scheme + that.conf.Broker()
	opts.AddBroker(brokerURL)
	opts.SetClientID(that.conf.ClientId())
	opts.SetUsername(that.conf.Username())
	opts.SetPassword(that.conf.Password())
	opts.SetProtocolVersion(uint(that.conf.Version()))
	opts.SetKeepAlive(time.Duration(that.conf.KeepAlive()))
	opts.SetCleanSession(that.conf.CleanSession())

	// // 2. 注册默认发布处理器 (处理所有下行消息并透传 voidObj)
	// opts.SetDefaultPublishHandler(func(c paho.Client, msg paho.Message) {})

	// 遗嘱设置
	if len(that.conf.Topics()) > 0 {
		opts.SetWill(that.conf.WillTopic(), string(that.conf.WillPayload()), that.conf.Qos(), that.conf.WillRetain())
	}

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(time.Second * 5) // 重连等待时间, SetConnectRetry(true)时才生效
	opts.SetConnectTimeout(time.Duration(that.conf.Timeout()))
	opts.SetPingTimeout(time.Duration(that.conf.Timeout()))
	// TLS 配置
	if that.conf.useTLS {
		tlsConfig := &tls.Config{}
		if that.conf.caCertPath != "" {
			caCert, err := os.ReadFile(that.conf.caCertPath)
			if err != nil {
				that.logf(klog.ErrorLevel, mqtt_tag, "Failed to read CA cert: %v", err)
				return err
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caCert) {
				that.logf(klog.ErrorLevel, mqtt_tag, "%v", ErrParseCA)
				return ErrParseCA
			}
			tlsConfig.RootCAs = certPool
		} else {
			that.logf(klog.ErrorLevel, mqtt_tag, "%v", ErrNoCAProvided)
			return ErrNoCAProvided
		}
		opts.SetTLSConfig(tlsConfig)
	}

	// 连接成功事件, 连接鉴权成功后才可以开始进行后续的指令操作
	opts.SetOnConnectHandler(func(c paho.Client) {
		if that.logf != nil {
			that.logf(klog.InfoLevel, mqtt_tag, "mqtt %s %s connect success", that.conf.ClientId(), that.conf.Broker())
		}

		that.connected.Store(true)
		that.syncSubscriptions() // 同步订阅视图
		// 触发 OnReady
		that.readyOnce.Do(func() {
			if that.conf.OnReady != nil {
				that.conf.OnReady(true)
			}
		})

	})

	opts.OnConnectionLost = func(c paho.Client, err error) {
		that.connected.Store(false)
		if that.logf != nil {
			that.logf(klog.WarnLevel, mqtt_tag, "MQTT connection lost: %s", err.Error())
		}
	}

	// 重连事件
	opts.SetReconnectingHandler(func(c paho.Client, option *paho.ClientOptions) {
		if that.logf != nil {
			that.logf(klog.DebugLevel, mqtt_tag, "mqtt %s %s reconnecting", that.conf.ClientId(), that.conf.Broker())
		}
	})

	// 全局 MQTT pub 消息处理, subscribe 操作时没有指定明确回调函数的, 都会走这里处理
	opts.SetDefaultPublishHandler(func(client paho.Client, msg paho.Message) {
		if that.logf != nil {
			that.logf(klog.DebugLevel, mqtt_tag, "mqtt %s %s receive message: %s", that.conf.ClientId(), that.conf.Broker(), string(msg.Payload()))
		}
	})

	client := paho.NewClient(opts)
	that.client = client
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		if that.logf != nil {
			that.logf(klog.ErrorLevel, mqtt_tag, "mqtt %s %s connect error: %s", that.conf.ClientId(), that.conf.Broker(), token.Error())
		}
	}

	that.startDrainPipe() // 开始异步发送协程

	return nil
}

func (that *MqttClient) Close() {
	// 将状态从 false 变为 true。如果已经是 true，说明 Close 已在执行，直接返回; 排水模式开启
	if that.draining.Swap(true) {
		return
	}

	// 通知 Broker 停止向此 ClientID 推送新消息，确保排水期间不会有新的干扰数据进入
	topics := that.conf.Topics()
	if len(topics) > 0 && that.connected.Load() && that.client != nil {
		token := that.client.Unsubscribe(topics...)
		token.WaitTimeout(time.Second * 2)
	}

	that.queue.Close() // 封锁队列

	// 等待排水模式完成 或20s超时
	done := make(chan struct{})
	go func() { that.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
	}

	if that.client != nil {
		that.client.Disconnect(250)
	}
	that.ctx.Cancel()
	that.ctx.Remove()
}

// Publish 业务层异步发送入口
func (that *MqttClient) Publish(msg *MqttMessage) bool {
	if that.draining.Load() || that.client == nil {
		return false
	}
	return that.queue.Enqueue(msg)
}

func (that *MqttClient) PublishMessage(topic string, message string) bool {
	msg := &MqttMessage{
		Topic:     topic,
		Duplicate: false,
		Retained:  true,
		Qos:       that.conf.Qos(),
		Payload:   []byte(message),
	}
	return that.Publish(msg)
}

///////////////////////////////////////////////////////////////

// syncSubscriptions 内部方法：对齐订阅视图
func (that *MqttClient) syncSubscriptions() {
	topicFilters := make(map[string]byte)
	topics := that.conf.Topics()
	if len(topics) == 0 {
		return
	}

	for _, topic := range topics {
		topicFilters[topic] = that.conf.Qos()
	}

	handler := that.conf.MainHandler()
	pahoHandler := func(c paho.Client, msg paho.Message) {
		if handler != nil {
			handler(c, &MqttMessage{
				Topic:     msg.Topic(),
				Payload:   msg.Payload(),
				Qos:       msg.Qos(),
				Retained:  msg.Retained(),
				Duplicate: msg.Duplicate(),
				MessageID: msg.MessageID(),
			})
		}
	}

	token := that.client.SubscribeMultiple(topicFilters, pahoHandler)
	// 阻塞等待订阅完成
	if token.Wait() && token.Error() != nil {
		if that.logf != nil {
			that.logf(klog.ErrorLevel, mqtt_tag, "mqtt %s subscribe fault: %s", that.conf.ClientId(), token.Error())
		}
	} else {
		if that.logf != nil {
			that.logf(klog.InfoLevel, mqtt_tag, "mqtt %s subscribe topics: [%s] finished", that.conf.ClientId(), strings.Join(topics, ", "))
		}
	}
}

// startDrainPipe 核心后台协程：负责消息的排队确认与重试
func (that *MqttClient) startDrainPipe() {
	that.wg.Add(1)
	go func() {
		defer that.wg.Done()
		buffer := make([]*MqttMessage, that.queueSize)
		for {
			n := that.queue.DequeueTo(buffer)
			if n <= 0 { // 队列已 Close 且数据排干
				break
			}
			for _, msg := range buffer[:n] {
				for {
					if that.connected.Load() {
						token := that.client.Publish(msg.Topic, msg.Qos, msg.Retained, msg.Payload)
						if token.WaitTimeout(time.Second*5) && token.Error() == nil {
							break // 发送成功，跳出当前消息的重试循环，处理 buffer 中的下一条
						}
					}
					// 主动退出或排水中且链路断开，则放弃重试
					if (that.ctx.Context().Err() != nil || that.draining.Load()) && !that.connected.Load() {
						return
					}
					time.Sleep(500 * time.Millisecond) // 等待500ms后重试
				}
			}
		}
	}()
}

///////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////
