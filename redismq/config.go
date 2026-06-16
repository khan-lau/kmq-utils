package redismq

import (
	"errors"

	"github.com/khan-lau/kutils/db/kredis"
)

const (
	RedisLogTag = "redismq"
)

var (
	ErrEmptyAddrs = errors.New("empty addrs")
)

/////////////////////////////////////////////////////////////

type ErrorCallbackFunc func(err error)
type EventCallbackFunc func(event any)
type ReadyCallbackFunc func(ready bool)
type MessageHandler func(voidObj any, msg *kredis.RedisMessage)

/////////////////////////////////////////////////////////////

type RedisConfig struct {
	Addrs    []string `json:"addrs"`
	Password string   `json:"password"`
	DB       int      `json:"db"`
	Retry    int      `json:"retry"`

	Topics []string `json:"topics"`

	OnError        ErrorCallbackFunc // 设置错误回调
	OnExit         EventCallbackFunc // 设置退出回调
	OnReady        ReadyCallbackFunc // 设置启动完成回调
	messageHandler MessageHandler    // 消息处理回调函数
}

func NewRedisConfig() *RedisConfig {
	return &RedisConfig{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
		Retry:    5,
		Topics:   []string{},
	}
}

func (that *RedisConfig) SetAddrs(addrs ...string) *RedisConfig {
	that.Addrs = addrs
	return that
}

func (that *RedisConfig) SetPassword(password string) *RedisConfig {
	that.Password = password
	return that
}

func (that *RedisConfig) SetDB(db int) *RedisConfig {
	that.DB = db
	return that
}

func (that *RedisConfig) SetRetry(retry int) *RedisConfig {
	that.Retry = retry
	return that
}

func (that *RedisConfig) SetTopics(topics ...string) *RedisConfig {
	that.Topics = topics
	return that
}

func (that *RedisConfig) AddTopic(topic string) *RedisConfig {
	that.Topics = append(that.Topics, topic)
	return that
}

func (that *RedisConfig) SetErrorCallback(callback ErrorCallbackFunc) *RedisConfig {
	that.OnError = callback
	return that
}

func (that *RedisConfig) SetExitCallback(callback EventCallbackFunc) *RedisConfig {
	that.OnExit = callback
	return that
}

func (that *RedisConfig) SetReadyCallback(callback ReadyCallbackFunc) *RedisConfig {
	that.OnReady = callback
	return that
}

func (that *RedisConfig) SetMessageHandler(handler MessageHandler) *RedisConfig {
	that.messageHandler = handler
	return that
}

func (that *RedisConfig) MainHandler() MessageHandler {
	return that.messageHandler
}
