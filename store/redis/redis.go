package redis

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/topfreegames/pitaya"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/topfreegames/pitaya/config"
	"github.com/topfreegames/pitaya/logger"
)

//redis连接类型
const (
	Single   = 0
	Cluster  = 1
	Sentinel = 2
)

//RedisStore redis结构
type RedisStore struct {
	tye int //类型 0单机 1集群
	// auth        bool                 //是否授权
	// pwd         string               //密码
	host        string               //单机服务器地址
	clusterHost []string             //集群服务器地址
	client      *redis.Client        //单机的前端
	cluster     *redis.ClusterClient //集群的前端
	locker      *redislock.Client
}

var (
	single *RedisStore
	once   sync.Once
)

//S 获取单例
func S() *RedisStore {
	once.Do(func() {
		single = &RedisStore{
			tye:  0,
			host: "localhost:6379",
		}
	})
	return single
}

//Locker 分布锁
func (s *RedisStore) Locker() *redislock.Client {
	return s.locker
}

//Client 获取redis链接
func (s *RedisStore) Client() *redis.Client {
	return s.client
}

//Connect 连接redis服务
func (s *RedisStore) Connect() error {
	var cfg *config.Config = pitaya.GetConfig()
	if cfg != nil {
		s.tye = cfg.GetInt("dataapp.store.redis.type")
		s.host = cfg.GetString("dataapp.store.redis.host")
		// s.auth = cfg.GetBool("store.redis.auth")
		// s.pwd = cfg.GetString("store.redis.pwd")
	}

	if s.tye == Single { //单机
		s.singleConnect()
		// 启用分布式锁
		s.locker = redislock.New(s.client)
	} else if s.tye == Cluster { //集群
		s.clusterConnect()
	} else {
		s.sentinelConnect()
	}

	return nil
}

func (s *RedisStore) singleConnect() (err error) {
	mc := redis.NewClient(&redis.Options{
		Addr: s.host,
		DB:   0,
		//命令执行失败时的重试策略
		MaxRetries:      0,                      // 命令执行失败时，最多重试多少次，默认为0即不重试
		MinRetryBackoff: 200 * time.Millisecond, //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 1 * time.Second,

		//超时
		// DialTimeout:  5 * time.Second, //连接建立超时时间，默认5秒。
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时

		//连接池
		PoolSize:     20, // 连接池最大socket连接数，默认为10倍CPU数， 10 * runtime.NumCPU
		MinIdleConns: 5,  //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量；。
		// MaxConnAge:         0 * time.Second,  //连接存活时长，从创建开始计时，超过指定时长则关闭连接，默认为0，即不关闭存活时长较长的连接
		// PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。

		//闲置连接检查包括IdleTimeout，MaxConnAge
		IdleTimeout: 5 * time.Minute, //闲置超时，默认5分钟，-1表示取消闲置超时检查
		// IdleCheckFrequency: 60 * time.Second, //闲置连接检查的周期，默认为1分钟，-1表示不做周期性检查，只在客户端获取连接时对闲置连接进行处理。
	})
	if _, err = mc.Ping(mc.Context()).Result(); err != nil {
		logger.Log.Errorf("RedisStore::SingleConnect single error, cann't ping the redis server[url:%s, error:%+v]", s.host, err)
	} else {
		logger.Log.Debugf("RedisStore::SingleConnect single successfully[url:%s]", s.host)
		s.client = mc
	}
	return
}

// sentinelConnect  哨兵模式
func (s *RedisStore) sentinelConnect() (err error) {
	mc := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    "master",
		SentinelAddrs: []string{":26379"},
	})
	if _, err = mc.Ping(mc.Context()).Result(); err != nil {
		logger.Log.Errorln("RedisStore::SentinelConnect single error, cann't ping the redis server[url:%s, error:%+v]", s.host, err)
	} else {
		logger.Log.Debugln("RedisStore::SentinelConnect single successfully[url:%s]", s.host)
		s.client = mc
	}
	return
}

//clusterConnect  集群模式
func (s *RedisStore) clusterConnect() (err error) {
	mc := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"},
	})
	if _, err = mc.Ping(mc.Context()).Result(); err != nil {
		logger.Log.Errorln("RedisStore::ClusterConnect single error, cann't ping the redis server[url:%s, error:%+v]", s.host, err)
	} else {
		logger.Log.Debugln("RedisStore::ClusterConnect single successfully[url:%s]", s.host)
		s.cluster = mc
	}
	return
}

//Disconnect 断开连接
func (s *RedisStore) Disconnect() (err error) {
	if s.client != nil {
		if err = s.client.Close(); err != nil {
			return err
		}
		s.client = nil
	}
	if s.cluster != nil {
		if err = s.cluster.Close(); err != nil {
			return err
		}
		s.cluster = nil
	}
	return
}

// Do 执行命令
func (s *RedisStore) Do(args ...interface{}) (interface{}, error) {
	ctx := context.Background()
	if strings.ToLower(args[0].(string)) == "hgetall" && len(args) == 2 {
		return s.client.HGetAll(ctx, args[1].(string)).Result()
	}
	cmd := redis.NewCmd(ctx, args...)
	if s.tye == Single || s.tye == Sentinel { //单机
		_ = s.client.Process(ctx, cmd)
	} else { //集群
		_ = s.cluster.Process(ctx, cmd)
	}
	return cmd.Result()
}

// HSetStruct 设置结构体
func (s *RedisStore) HSetStruct(key string, result interface{}) (err error) {
	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, Args{CmdHSet}.Add(key).AddFlat(result)...)

	if s.tye == Single || s.tye == Sentinel { //单机
		err = s.client.Process(ctx, cmd)
	} else { //集群
		err = s.cluster.Process(ctx, cmd)
	}
	if err == nil {
		_, err = cmd.Result()
	}
	return err
}

// HSetStruct 设置结构体
func (s *RedisStore) HGetAll(key string, result interface{}) (err error) {
	ctx := context.Background()
	cmd := redis.NewStatusCmd(ctx, Args{CmdHSet}.Add(key).AddFlat(result)...)

	if s.tye == Single || s.tye == Sentinel { //单机
		err = s.client.Process(ctx, cmd)
	} else { //集群
		err = s.cluster.Process(ctx, cmd)
	}
	if err == nil {
		_, err = cmd.Result()
	}
	return err
}
