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

const (
	DefaultBatchSize = 32 // 默认批量大小
)

type RedisPub struct {
	ctx          *kcontext.ContextNode
	redisHandler *RedisToolkit

	started    atomic.Bool                                  // 是否已经启动过（防止重复启动 goroutine）
	draining   atomic.Bool                                  // 正在关闭中, true 表示正在关闭中, false 表示正常运行中, 默认为false
	connected  atomic.Bool                                  // 是否已连接, true 表示已连接, false 表示未连接
	queue      *ksync.LockedRingBuffer[kredis.RedisMessage] // 消息队列
	bufferSize uint                                         // 缓冲区大小
	wg         sync.WaitGroup
	conf       *RedisConfig
	logf       klog.AppLogFuncWithTag
}

func NewRedisPub(ctx *kcontext.ContextNode, queueSize uint, conf *RedisConfig, logf klog.AppLogFuncWithTag) (*RedisPub, error) {
	subCtx := ctx.NewChild("redismq_pubsub")

	if len(conf.Addrs) == 0 {
		if logf != nil {
			logf(klog.ErrorLevel, RedisLogTag, 0, "redis config addrs is empty")
		}
		return nil, ErrEmptyAddrs
	}

	// redisHD := kredis.NewKRedis(subCtx, conf.Addrs[0], "", conf.Password, conf.DB)
	redisHD := NewRedisToolkit(subCtx, conf, logf)

	queue, err := ksync.NewLockedRingBuffer[kredis.RedisMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, RedisLogTag, 0, "Create redisPub queue failed: %s", err.Error())
		}
		return nil, err
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

	return redisPs, nil
}

func (that *RedisPub) Start() {
	if !that.started.CompareAndSwap(false, true) {
		that.log(klog.WarnLevel, "RedisPub already started")
		return
	}

	that.log(klog.InfoLevel, "RedisPub starting...")

	that.reconnectLoop(func() {
		that.log(klog.InfoLevel, "Producer mode connected, ready to publish")
		// 准备发送信息
		that.startPublish()
	})

	that.log(klog.InfoLevel, "RedisPub stopped")
}

func (that *RedisPub) PublishMessage(topic string, message string) bool {
	msg := kredis.RedisMessage{
		Topic:   topic,
		Message: message,
	}
	return that.Publish(msg)
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

	that.log(klog.InfoLevel, "Client stopped")
}

func (that *RedisPub) Publish(msg kredis.RedisMessage) bool {
	if that.draining.Load() {
		that.log(klog.WarnLevel, "in draining mode, publish rejected: %s", msg.Topic)
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
			that.log(klog.InfoLevel, "draining → exit reconnect loop")

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
			that.log(klog.InfoLevel, "connected")
			if onConected != nil {
				onConected()
			}
			continue
		}

		err := fmt.Errorf("connect to redis [%s] - %d faulted", strings.Join(that.conf.Addrs, ", "), that.conf.DB)
		that.log(klog.ErrorLevel, "%v", err)

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
		// that.redisHandler = kredis.NewKRedis(subCtx, that.conf.Addrs[0], "", that.conf.Password, int(that.conf.DB))
		that.redisHandler = NewRedisToolkit(subCtx, that.conf, that.logf)
	}

	if err := that.redisHandler.PingE(); err != nil { //探测连接失败
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

	batchSize := max(int(that.bufferSize/4), DefaultBatchSize)
	batchArray := make([]kredis.RedisMessage, batchSize)
	for {
		if that.draining.Load() { // 正在关闭中，退出循环
			// 循环处理完剩余队列后，再退出循环, 确保 quit 之前的数据全部发出
			for {
				remainMsgs, n := that.queue.DequeueBatchNoWait(int(that.bufferSize))
				if n <= 0 {
					break
				}
				that.log(klog.WarnLevel, "remainMsgs: %d", n)
				that.PublishArray(remainMsgs)
			}

			return
		}

		// 如果连接已断开且不是 draining，退出让新 goroutine 处理
		if !that.connected.Load() && !that.draining.Load() {
			return // 连接断开，退出等待重连后的新 goroutine
		}

		n, isValidQueue := that.queue.TryDequeueTo(batchArray)
		if n > 0 {
			that.PublishArray(batchArray[:n])
		} else if isValidQueue {
			time.Sleep(10 * time.Millisecond) // 避免 busy loop
			continue
		}

		// 队列已关闭或缓冲区为nil, 则直接返回
		if !isValidQueue {
			return
		}
	}
}

func (that *RedisPub) PublishArray(messages []kredis.RedisMessage) {
	if len(messages) == 0 {
		return
	}

	const maxDrainRetries = 3  // 排水期间 PUBLISH 失败允许的最大重试次数, 防止 redis 不可用时无限阻塞导致进程无法退出
	const retryInterval = 1000 // ms
	drainRetries := 0          // current retries in draining mode

	// 排水专用ctx: 独立ctx树, 不随父ctx取消, 在整个排水过程中复用
	drainCtx := kcontext.NewContextTree("redismq_pub_drain").GetRoot()
	for {
		if that.redisHandler != nil {
			var errs []error
			if that.draining.Load() {
				// 排水模式: 必须使用独立的排水ctx. 内部ctx(redis_toolkit)可能已被取消, 用内部ctx会导致 PUBLISH 立即失败
				errs = that.redisHandler.PublishArrayWithCtx(drainCtx, messages)
			} else {
				errs = that.redisHandler.PublishArray(messages)
			}

			if len(errs) == 0 {
				return // 发送成功
			} else if !that.draining.Load() {
				that.onError(errs[0]) // 正常模式: 通知上层错误(不停止服务)
			}
		}

		// 排水期间: 有限次重试后放弃; 正常模式: 无限重试直到成功
		if that.draining.Load() {
			drainRetries++
			if drainRetries >= maxDrainRetries {
				that.log(klog.WarnLevel, "Publish in draining mode failed after %d retries, %d messages dropped", maxDrainRetries, len(messages))
				return
			}
		}

		time.Sleep(time.Duration(retryInterval) * time.Millisecond)
	}
}

func (that *RedisPub) onError(err error) {
	if that.conf.OnError != nil {
		that.conf.OnError(err)
	}

	that.log(klog.WarnLevel, "%v", err)
	// 重要：只标记连接断开，让 reconnectLoop 自动重连
	// 不要调用 stop()，否则会永久停止
	that.connected.Store(false)
}

// log 日志记录, 会自动添加 RedisLogTag
//
//go:inline
func (that *RedisPub) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RedisLogTag, 1, format, args...)
	}
}
