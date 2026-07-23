package redismq

import (
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/db/kredis"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	RedisToolLogTag = "RedisToolkit"
)

////////////////////////////////////////////////////////////

// Redis客户端封装结构体, 同时包含集群 和 单节点, 用于统一处理不同类型的Redis客户端
type ClientHandlerWrapper struct {
	Client        *kredis.KRedis
	ClusterClient *kredis.KRedisCluster
}

func NewClientHandlerWrapper(ctx *kcontext.ContextNode, conf *RedisConfig) *ClientHandlerWrapper {
	if len(conf.Addrs) == 0 {
		return nil
	}

	that := &ClientHandlerWrapper{}
	if len(conf.Addrs) == 1 {
		that.Client = kredis.NewKRedis(ctx, conf.Addrs[0], "", conf.Password, int(conf.DB))
	} else {
		that.ClusterClient = kredis.NewKRedisCluster(ctx, conf.Addrs, "", conf.Password, int(conf.DB))
	}
	return that
}

func (that *ClientHandlerWrapper) Ping() bool {
	if that.Client != nil {
		return that.Client.Ping()
	}
	if that.ClusterClient != nil {
		return that.ClusterClient.Ping()
	}
	return false
}

// SyncPSubscribeReceive 同步订阅Redis主题, 并处理消息
//
//	参数
//	- @param callback: 消息处理回调函数, 参数为(err error, topic string, payload any)
//	- @param topics: 要订阅的主题列表
func (that *ClientHandlerWrapper) SyncPSubscribeReceive(callback func(err error, topic string, payload any), topics ...string) {
	if that.Client != nil {
		that.Client.SyncPSubscribeReceive(callback, topics...)
	}
	if that.ClusterClient != nil {
		that.ClusterClient.SyncPSubscribeReceive(callback, topics...)
	}
}

func (that *ClientHandlerWrapper) Publish(topic string, message any) error {
	if that.Client != nil {
		return that.Client.Publish(topic, message)
	}
	if that.ClusterClient != nil {
		return that.ClusterClient.Publish(topic, message)
	}
	return ErrWrapError
}

func (that *ClientHandlerWrapper) PublishArray(messages []*kredis.RedisMessage) []error {
	if that.Client != nil {
		return that.Client.PublishArray(messages)
	}
	if that.ClusterClient != nil {
		return that.ClusterClient.PublishArray(messages)
	}
	return []error{ErrWrapError}
}

// Stop 停止Redis客户端, 并释放资源
func (that *ClientHandlerWrapper) Stop() {
	if that.Client != nil {
		that.Client.Stop()
		that.Client = nil
	}
	if that.ClusterClient != nil {
		that.ClusterClient.Stop()
		that.ClusterClient = nil
	}
}

////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////

type RedisToolkit struct {
	ctx *kcontext.ContextNode

	redisHandler *ClientHandlerWrapper
	conf         *RedisConfig
	logf         klog.AppLogFuncWithTag
}

// 创建RedisToolkit实例
//
//	参数
//	- @param ctx: 上下文节点
//	- @param conf: Redis配置, 配置中的Addrs如果数量大于1, 则使用ClusterAPI, 负责使用普通ClientAPI
//	- @param logf: 日志回调函数
func NewRedisToolkit(ctx *kcontext.ContextNode, conf *RedisConfig, logf klog.AppLogFuncWithTag) *RedisToolkit {
	if len(conf.Addrs) == 0 {
		if logf != nil {
			logf(klog.ErrorLevel, RedisToolLogTag, 0, "redis config addrs is empty")
		}
		return nil
	}

	subCtx := ctx.NewChild("redis_toolkit")
	handler := NewClientHandlerWrapper(subCtx, conf)
	redisPs := &RedisToolkit{
		ctx:          ctx,
		redisHandler: handler,
		conf:         conf,
		logf:         logf,
	}

	return redisPs
}

func (that *RedisToolkit) Ping() bool {
	if len(that.conf.Addrs) == 0 {
		that.log(klog.ErrorLevel, "redis config addrs is empty")
		return false
	}
	return that.redisHandler.Ping()
}

// SyncPSubscribeReceive 同步订阅Redis主题, 并处理消息
func (that *RedisToolkit) SyncPSubscribeReceive(callback func(err error, topic string, payload any), topics ...string) {
	if len(that.conf.Addrs) == 0 {
		that.log(klog.ErrorLevel, "redis config addrs is empty")
		return
	}
	that.redisHandler.SyncPSubscribeReceive(callback, topics...)
}

// PublishArray 发布Redis主题, 并将数组转换为JSON字符串
func (that *RedisToolkit) PublishArray(messages []*kredis.RedisMessage) []error {
	if len(that.conf.Addrs) == 0 {
		that.log(klog.ErrorLevel, "%v", ErrEmptyAddrs)
		return []error{ErrEmptyAddrs}
	}
	return that.redisHandler.PublishArray(messages)
}

func (that *RedisToolkit) Stop() {
	// // TODO：测试期间用于统计数据. 打印当前统计信息
	// that.PrintStats()

	that.redisHandler.Stop()
}

// log 日志记录, 会自动添加 RedisToolLogTag
//
//go:inline
func (that *RedisToolkit) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RedisToolLogTag, 1, format, args...)
	}
}
