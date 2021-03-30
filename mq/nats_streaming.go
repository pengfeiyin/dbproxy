package mq

import (
	"context"
	"dataapp/agent"
	"dataapp/store/mongo"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/stan.go"
	"github.com/topfreegames/pitaya"
	"github.com/topfreegames/pitaya/config"
	"github.com/topfreegames/pitaya/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	single *NatsStreaming
	once   sync.Once
)
var ReconnTimes = 3 //重连次数

//管理nats_streaming的发布与订阅
type NatsStreaming struct {
	Url       string    //nats服务器连接数据
	ClusterID string    //集群ID
	ClientID  string    //客户端ID
	Durable   string    //持久性数据标识名字
	Subject   string    //用于订阅发布的topic标识
	Conn      stan.Conn //连接
}

//S 单例
func S() *NatsStreaming {
	once.Do(func() {
		single = &NatsStreaming{
			Url:       "nats://localhost:4222",
			ClusterID: "nats-cluster",
			ClientID:  "dataapp-sync",
			Subject:   "mongosync",
			Durable:   "mongo-data-msg",
		}
	})
	return single
}

//连接
func (ns *NatsStreaming) Connect() (err error) {
	var cfg *config.Config = pitaya.GetConfig()
	if cfg != nil && cfg.GetString("dataapp.mq.nats_streaming.url") != "" {
		ns.Url = cfg.GetString("dataapp.mq.nats_streaming.url")
		ns.ClusterID = cfg.GetString("dataapp.mq.nats_streaming.cluster_id")
		ns.ClientID = cfg.GetString("dataapp.mq.nats_streaming.client_id")
		ns.Durable = cfg.GetString("dataapp.mq.nats_streaming.durable")
		ns.Subject = cfg.GetString("dataapp.mq.nats_streaming.subject")
	}

	if ns.Conn == nil {
		loop := 0
		//连接失去后的处理函数
		connLostFunc := func(_ stan.Conn, reason error) {
			ns.Disconnect()
			ns.Connect()
		}
		for loop < ReconnTimes {
			ns.Conn, err = stan.Connect(ns.ClusterID, ns.ClientID, stan.NatsURL(ns.Url), stan.SetConnectionLostHandler(connLostFunc))
			if err == nil {
				//启动订阅函数去订阅消息
				if err = ns.Subscribe(); err != nil {
					fmt.Printf("%v\n", err)
				}
				break
			}
			ns.Conn = nil
			loop++
		}
	}
	return
}

//关闭
func (ns *NatsStreaming) Disconnect() (err error) {
	if ns.Conn != nil {
		err = ns.Conn.Close()
		if err == nil {
			ns.Conn = nil
		}
	}
	return
}

//发布消息
func (ns *NatsStreaming) PublishMsg(msg agent.AsyncMsg) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("NatsStreaming::PublishMsg panic:%+v", e)
			ns.Disconnect()
		}
	}()

	var jByte []byte

	if jByte, err = json.Marshal(msg); err == nil {
		err = ns.Conn.Publish(ns.Subject, jByte)
		if err != nil { //失败后关闭，待下次调用再重连
			ns.Disconnect()
		}
	}
	return
}

//订阅消息
func (ns *NatsStreaming) Subscribe() (err error) {
	subfunc := func(msg *stan.Msg) { //回调函数
		aMsg := &agent.AsyncMsg{}
		if err := json.Unmarshal(msg.Data, aMsg); err == nil {
			//参数校验，返回的结果必须为地址传值
			if aMsg.Query.DbName == "" || aMsg.Query.TblName == "" {
				logger.Log.Errorf("NatsStreaming::Subscribe function parameter error, not about table name[parameter:%+v]", aMsg.Query)
				return
			}
			collection := mongo.S().C(aMsg.Query.TblName, aMsg.Query.DbName)
			if collection == nil {
				logger.Log.Errorf("NatsStreaming::Subscribe error  table [%s.%s] not exist", aMsg.Query.DbName, aMsg.Query.TblName)
				return
			}

			if aMsg.OptType == agent.SET {
				filter := agent.ParseFilter(aMsg.Query)
				updateData := bson.M{"$set": aMsg.Val}
				collection.UpdateMany(context.TODO(), filter, updateData, options.Update().SetUpsert(true))
			}
			if aMsg.OptType == agent.DEL {
				filter := agent.ParseFilter(aMsg.Query)
				collection.DeleteMany(context.TODO(), filter)
			}
			if aMsg.OptType == agent.INS {
				collection.InsertOne(context.TODO(), aMsg.Val)
			}
		}
	}
	// 以手动ack模式订阅, 且设置 AckWait 时间至60s
	// aw, _ := time.ParseDuration("60s")
	// ns.Conn.Subscribe(ns.Subject, subfunc, stan.DurableName(ns.Durable), stan.MaxInflight(25),
	// 	stan.SetManualAckMode(),
	// 	stan.AckWait(aw))
	_, err = ns.Conn.Subscribe(ns.Subject, subfunc, stan.DurableName(ns.Durable))
	if err != nil {
		panic(err)
	}
	return
}
