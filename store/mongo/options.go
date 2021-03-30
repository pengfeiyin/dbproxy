package mongo

import (
	"time"
)

//
const (
	DefaultConnectTimeout   = 5 * time.Second // 连接超时时间
	DefaultSocketTimeout    = 5 * time.Second
	DefaultMaxConnIdleTime  = 3 * time.Second // 最大空闲时间
	DefaultReadWriteTimeout = 3 * time.Second // 读写超时时间
)

//Option func
type Option func(o *Options)

//Options 结构
type Options struct {
	RawURL           string
	DbName           string
	MinPoolSize      uint64        // 最小连接池大小
	MaxPoolSize      uint64        // 最大连接池大小
	ConnectTimeout   time.Duration // 连接超时时间
	SocketTimeout    time.Duration
	MaxConnIdleTime  time.Duration // 最大空闲时间
	ReadWriteTimeout time.Duration // 读写超时时间
}

//Init Mongo
func (o *Options) Init(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

func newOptions(opts ...Option) *Options {
	o := &Options{
		RawURL:           Opts.MongoURL,
		MinPoolSize:      Opts.MongoMinPool,
		MaxPoolSize:      Opts.MongoMaxPool,
		ConnectTimeout:   DefaultConnectTimeout,
		SocketTimeout:    DefaultSocketTimeout,
		MaxConnIdleTime:  DefaultMaxConnIdleTime,
		ReadWriteTimeout: DefaultReadWriteTimeout,
	}
	o.Init(opts...)
	return o
}

//WithURL Mongo
func WithURL(url string) Option {
	return func(o *Options) {
		o.RawURL = url
	}
}

//WithDbName Mongo
func WithDbName(db string) Option {
	return func(o *Options) {
		o.DbName = db
	}
}

//WithMinPoolSize Mongo
func WithMinPoolSize(size uint64) Option {
	return func(o *Options) {
		o.MinPoolSize = size
	}
}

//WithMaxPoolSize Mongo
func WithMaxPoolSize(size uint64) Option {
	return func(o *Options) {
		o.MaxPoolSize = size
	}
}

//WithConnectTimeout Mongo
func WithConnectTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.ConnectTimeout = t
	}
}

//WithSocketTimeout Mongo
func WithSocketTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.SocketTimeout = t
	}
}

//WithMaxConnIdleTime Mongo
func WithMaxConnIdleTime(t time.Duration) Option {
	return func(o *Options) {
		o.MaxConnIdleTime = t
	}
}

//WithReadWriteTimeout Options
func WithReadWriteTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.ReadWriteTimeout = t
	}
}
