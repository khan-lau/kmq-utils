package redismq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/db/kredis"
	klog "github.com/khan-lau/kutils/klogger"
)

// - 一旦客户端发送了 SUBSCRIBE 或 PSUBSCRIBE 命令，它就进入了订阅状态
// - 在该状态下，客户端只能执行：SUBSCRIBE、PSUBSCRIBE、UNSUBSCRIBE、PUNSUBSCRIBE、PING 和 QUIT
// - 如果在此连接上尝试执行 PUBLISH、SET、GET 等普通指令，Redis 会直接返回错误，或者在某些旧版本中完全不响应

type RedisSub struct {
	ctx          *kcontext.ContextNode
	conf         *RedisConfig
	redisHandler *kredis.KRedis

	started   atomic.Bool // 是否已经启动过（防止重复启动 goroutine）
	draining  atomic.Bool // 正在关闭中, true 表示正在关闭中, false 表示正常运行中, 默认为false
	connected atomic.Bool // 是否已连接, true 表示已连接, false 表示未连接

	bufferSize uint // 缓冲区大小
	wg         sync.WaitGroup
	logf       klog.AppLogFuncWithTag
}

func NewRedisSub(ctx *kcontext.ContextNode, bufferSize uint, conf *RedisConfig, logf klog.AppLogFuncWithTag) (*RedisSub, error) {
	if len(conf.Addrs) == 0 {
		if logf != nil {
			logf(klog.ErrorLevel, RedisLogTag, "redis config addrs is empty")
		}
		return nil, ErrEmptyAddrs
	}

	subCtx := ctx.NewChild("redismq_pubsub")
	redisHD := kredis.NewKRedis(subCtx, conf.Addrs[0], "", conf.Password, conf.DB)
	redisPs := &RedisSub{
		ctx:          ctx,
		conf:         conf,
		redisHandler: redisHD,
		started:      atomic.Bool{}, // 默认为false
		draining:     atomic.Bool{}, // 默认为false
		connected:    atomic.Bool{}, // 默认为false
		bufferSize:   bufferSize,
		wg:           sync.WaitGroup{}, //
		logf:         logf,
	}

	return redisPs, nil
}

func (that *RedisSub) Subscribe() {
	go that.SyncSubscribe()
}

func (that *RedisSub) SyncSubscribe() {
	if !that.started.CompareAndSwap(false, true) {
		if that.logf != nil {
			that.logf(klog.WarnLevel, RedisLogTag, "RedisSub already started")
		}
		return
	}

	if that.logf != nil {
		that.logf(klog.InfoLevel, RedisLogTag, "RedisSub starting...")
	}

	that.reconnectLoop(func() {
		if that.logf != nil {
			that.logf(klog.InfoLevel, RedisLogTag, "Consumer mode connected, starting subscription...")
		}
		that.startSubscription() // 连接成功后启动订阅
	})

	if that.logf != nil {
		that.logf(klog.InfoLevel, RedisLogTag, "RedisSub stopped")
	}
}

func (that *RedisSub) Close() {
	that.draining.Store(true)
	that.connected.Store(false)
	if that.redisHandler != nil {
		that.redisHandler.Stop()
	}
	that.ctx.Cancel()
	that.ctx.Remove()
	that.wg.Wait()

	if that.logf != nil {
		that.logf(klog.InfoLevel, RedisLogTag, "Client stopped")
	}
}

///////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////

// 重新连接循环，每隔一段时间尝试重连数据库。如果已经关闭，则退出循环。
func (that *RedisSub) reconnectLoop(onConected func()) {
	const reconnectInterval = 5000 // 重连间隔 可配置为 3s、10s、15s 等
	const checkInterval = 10000    // 检查间隔 可配置为 10s、20s 等

	for {
		if that.draining.Load() {
			if that.logf != nil {
				that.logf(klog.InfoLevel, RedisLogTag, "draining → exit reconnect loop")
			}
			// 正在关闭中，退出循环
			// 清理资源

			// err := fmt.Errorf("client cancel db start")
			// that.onError(err)
			return
		}

		if that.connected.Load() {
			time.Sleep(checkInterval * time.Millisecond) // 已连接，定期检查或等待
			continue
		}

		// 尝试连接
		if that.dbConnect() {
			if that.logf != nil {
				that.logf(klog.InfoLevel, RedisLogTag, "connected")
			}
			if onConected != nil {
				onConected()
			}
			continue
		}

		err := fmt.Errorf("connect to redis %s - %d faulted", that.conf.Addrs[0], that.conf.DB)
		if that.logf != nil {
			that.logf(klog.ErrorLevel, RedisLogTag, "%v", err.Error())
		}

		time.Sleep(reconnectInterval * time.Millisecond)
	}
}

func (that *RedisSub) dbConnect() bool {
	if nil == that.redisHandler {
		ctxName := "redismq_pubsub"
		subCtx, _ := that.ctx.FindNodeByName(ctxName, "")
		if subCtx != nil {
			subCtx.Cancel()
			subCtx.Remove()
		}
		subCtx = that.ctx.NewChild(ctxName)
		that.redisHandler = kredis.NewKRedis(subCtx, that.conf.Addrs[0], "", that.conf.Password, int(that.conf.DB))
	}

	if !that.redisHandler.Ping() { //探测连接失败
		that.connected.Store(false)
		that.redisHandler.Stop()
		that.redisHandler = nil
		return false
	}
	that.connected.Store(true)

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	return true
}

func (that *RedisSub) onError(err error) {
	if that.conf.OnError != nil {
		that.conf.OnError(err)
	}
	if that.logf != nil {
		that.logf(klog.WarnLevel, RedisLogTag, "%v", err)
	}
	// 重要：只标记连接断开，让 reconnectLoop 自动重连
	// 不要调用 stop()，否则会永久停止
	that.connected.Store(false)
}

func (that *RedisSub) startSubscription() {
	if that.redisHandler == nil {
		return
	}

	that.wg.Add(1)
	defer that.wg.Done()

	handler := that.conf.MainHandler()
	msgHandler := func(voidObj any, msg *kredis.RedisMessage) {
		if nil != handler {
			handler(voidObj, msg)
		}
	}

	that.redisHandler.SyncPSubscribeWithChanSize(1000, int(that.bufferSize),
		func(err error, topic string, payload any) {
			if err != nil {
				that.onError(err)

			} else {
				msg, err := that.receivedMessage(topic, payload)
				if nil != err {
					if that.logf != nil {
						that.logf(klog.WarnLevel, RedisLogTag, "Subscribe reids topic error: %v", err)
					}
				} else {
					msgHandler(that, msg)
				}
			}

		}, that.conf.Topics...,
	)
}

func (that *RedisSub) receivedMessage(topic string, payload any) (*kredis.RedisMessage, error) {
	s, ok := payload.(string)
	if ok {
		return &kredis.RedisMessage{Topic: topic, Message: s}, nil
	} else {
		return nil, fmt.Errorf("payload data type unknown")
	}
}
