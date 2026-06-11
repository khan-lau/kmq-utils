package redismq

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/db/kredis"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
)

type RedisPub struct {
	ctx          *kcontext.ContextNode
	redisHandler *kredis.KRedis

	started    atomic.Bool                                   // 是否已经启动过（防止重复启动 goroutine）
	draining   atomic.Bool                                   // 正在关闭中, true 表示正在关闭中, false 表示正常运行中, 默认为false
	connected  atomic.Bool                                   // 是否已连接, true 表示已连接, false 表示未连接
	queue      *ksync.LockedRingBuffer[*kredis.RedisMessage] // 消息队列
	bufferSize uint                                          // 缓冲区大小
	wg         sync.WaitGroup
	conf       *RedisConfig
	logf       klog.AppLogFuncWithTag
}

func NewRedisPub(ctx *kcontext.ContextNode, queueSize uint, conf *RedisConfig, logf klog.AppLogFuncWithTag) *RedisPub {
	subCtx := ctx.NewChild("redismq_pubsub")

	if len(conf.Addrs) == 0 {
		if logf != nil {
			logf(klog.ErrorLevel, redis_tag, "redis config addrs is empty")
		}
		return nil
	}

	redisHD := kredis.NewKRedis(subCtx, conf.Addrs[0], "", conf.Password, conf.DB)

	queue, err := ksync.NewLockedRingBuffer[*kredis.RedisMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, redis_tag, "Create redisPub queue failed: %s", err.Error())
		}
		return nil
	}

	redisPs := &RedisPub{
		ctx:          ctx,
		redisHandler: redisHD,
		started:      atomic.Bool{}, // 默认为false
		draining:     atomic.Bool{}, // 默认为false
		connected:    atomic.Bool{}, // 默认为false
		queue:        queue,
		bufferSize:   queueSize,
		wg:           sync.WaitGroup{}, //
		conf:         conf,
		logf:         logf,
	}

	return redisPs
}

func (that *RedisPub) Close() {
	that.draining.Store(true)
	that.connected.Store(false)

	that.wg.Wait()

	that.queue.Close() // 关闭队列

	if that.redisHandler != nil {
		that.redisHandler.Stop()
	}

	that.ctx.Cancel()
	that.ctx.Remove()

	if that.logf != nil {
		that.logf(klog.InfoLevel, redis_tag, "Client stopped")
	}
}

func (that *RedisPub) Start() {
	if !that.started.CompareAndSwap(false, true) {
		if that.logf != nil {
			that.logf(klog.WarnLevel, redis_tag, "RedisPub already started")
		}
		return
	}

	if that.logf != nil {
		that.logf(klog.InfoLevel, redis_tag, "RedisPub starting...")
	}

	that.reconnectLoop(func() {
		if that.logf != nil {
			that.logf(klog.InfoLevel, redis_tag, "Producer mode connected, ready to publish")
		}
		// 准备发送信息
		that.startPublish()
	})

	if that.logf != nil {
		that.logf(klog.InfoLevel, redis_tag, "RedisPub stopped")
	}
}

func (that *RedisPub) PublishMessage(topic string, message string) bool {
	msg := &kredis.RedisMessage{
		Topic:   topic,
		Message: message,
	}
	return that.Publish(msg)
}

func (that *RedisPub) Publish(msg *kredis.RedisMessage) bool {
	if that.draining.Load() {
		if that.logf != nil {
			that.logf(klog.WarnLevel, redis_tag, "in draining mode, publish rejected: %s", msg.Topic)
		}
		return false
	}

	if that.queue != nil {
		return that.queue.Enqueue(msg)
	}
	return false
}

///////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////

// 重新连接循环，每隔一段时间尝试重连数据库。如果已经关闭，则退出循环。
func (that *RedisPub) reconnectLoop(onConected func()) {
	const reconnectInterval = 5000 // 重连间隔 可配置为 3s、10s、15s 等
	const checkInterval = 10000    // 检查间隔 可配置为 10s、20s 等

	for {
		if that.draining.Load() {
			if that.logf != nil {
				that.logf(klog.InfoLevel, redis_tag, "draining → exit reconnect loop")
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
				that.logf(klog.InfoLevel, redis_tag, "connected")
			}
			if onConected != nil {
				onConected()
			}
			continue
		}

		err := fmt.Errorf("connect to redis [%s] - %d faulted", strings.Join(that.conf.Addrs, ", "), that.conf.DB)
		if that.logf != nil {
			that.logf(klog.ErrorLevel, redis_tag, "%v", err)
		}

		time.Sleep(reconnectInterval * time.Millisecond)
	}
}

func (that *RedisPub) dbConnect() bool {
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

func (that *RedisPub) startPublish() {
	if that.redisHandler == nil {
		return
	}

	that.wg.Add(1)       // 注册到 WaitGroup
	defer that.wg.Done() // 退出时通知完成

	for {
		if that.draining.Load() { // 正在关闭中，退出循环
			// 处理完剩余队列后，再退出循环
			if remainLen := that.queue.Len(); remainLen > 0 {
				remainMsgs, n := that.queue.DequeueBatchNoWait(int(that.bufferSize))
				if n > 0 && that.logf != nil {
					that.logf(klog.WarnLevel, redis_tag, "remainMsgs: %d", n)
				}
				for _, msg := range remainMsgs {
					that.publishData(msg.Topic, msg.Message)
				}
			}

			return
		}

		// 如果连接已断开且不是 draining，退出让新 goroutine 处理
		if !that.connected.Load() && !that.draining.Load() {
			return // 连接断开，退出等待重连后的新 goroutine
		}

		if msg, ok := that.queue.TryDequeue(); ok {
			that.publishData(msg.Topic, msg.Message)
		}
	}
}

func (that *RedisPub) publishData(topic string, message string) {
	msg := &kredis.RedisMessage{
		Topic:   topic,
		Message: message,
	}
	if that.draining.Load() {
		// === 排水模式：只发一次 ===
		handler := that.redisHandler
		if handler == nil {
			that.logf(klog.WarnLevel, redis_tag, "redisHandler is nil in draining mode")
			return
		}
		if err := handler.Publish(msg.Topic, msg.Message); err != nil {
			if that.logf != nil {
				that.logf(klog.WarnLevel, redis_tag, "Publish in draining mode failed (one shot): %v", err)
			}
		}
		return
	}
	// === 正常模式：失败后死循环重试，直到成功 ===
	const maxRetryInterval = 1000

	for {
		// 如果在重试过程中被外部 Stop()，则立即退出
		if that.draining.Load() {
			if that.logf != nil {
				that.logf(klog.InfoLevel, redis_tag, "draining detected during retry, stop publishing")
			}
			return
		}

		if that.redisHandler != nil {
			err := that.redisHandler.Publish(msg.Topic, msg.Message)
			if err == nil {
				// 发送成功，退出重试循环
				return
			}

			// // 发送失败，记录日志并等待后重试
			// if that.logf != nil {
			// 	that.logf(klog.WarnLevel, redis_tag, "Publish failed, will retry: %v", err)
			// }
			that.onError(err) // 通知上层错误（但不停止服务）
		}

		// 避免 CPU 空转，失败后等待一段时间再重试
		time.Sleep(time.Duration(maxRetryInterval) * time.Millisecond)
	}
}

func (that *RedisPub) onError(err error) {
	if that.conf.OnError != nil {
		that.conf.OnError(err)
	}
	if that.logf != nil {
		that.logf(klog.WarnLevel, redis_tag, "%v", err)
	}
	// 重要：只标记连接断开，让 reconnectLoop 自动重连
	// 不要调用 stop()，否则会永久停止
	that.connected.Store(false)
}
