package servers

import (
	"context"
	"dataapp/agent"
	"dataapp/mq"
	"dataapp/protos"
	"dataapp/store/mongo"
	"dataapp/store/redis"
	"dataapp/util/dtype"
	"encoding/json"
	"fmt"

	"github.com/topfreegames/pitaya/component"
	"github.com/topfreegames/pitaya/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Empty struct {
	component.Base
}

type MongoStore struct {
	component.Base
}

type RedisStore struct {
	component.Base
}

// FindOne 从mongoDB中查找单条
func (m *MongoStore) FindOne(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	var err error = nil
	reply := protos.StoreResponse{}
	condition := agent.QueryPara{}

	if err = json.Unmarshal([]byte(req.Condition), &condition); err != nil {
		return &reply, err
	}

	defer func() { //捕获异常
		if e := recover(); e != nil {
			err = fmt.Errorf("MongoStore::FindOne panic:%+v", e)
		}
	}()

	//参数校验，返回的结果必须为地址传值
	if condition.DbName == "" || condition.TblName == "" {
		err = fmt.Errorf("MongoStore::FindOne function parameter error, not about table name[parameter:%+v]", condition)
		return &reply, err
	}

	filter := agent.ParseFilter(condition)
	options := agent.ParseOneOptions(condition)
	result := bson.M{}

	if err = mongo.S().FindOne(condition.TblName, condition.DbName, filter, &result, options); err == nil {
		reply.Data = string(dtype.ToJson(result))
	}

	return &reply, err
}

// FindAll 从mongoDB中查找多条
func (m *MongoStore) FindAll(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	var err error = nil

	reply := protos.StoreResponse{}
	condition := agent.QueryPara{}

	if err := json.Unmarshal([]byte(req.Condition), &condition); err != nil {
		return &reply, err
	}

	defer func() { //捕获异常
		if e := recover(); e != nil {
			err = fmt.Errorf("MongoStore::FindAll panic:%+v", e)
		}
	}()

	//参数校验，返回的结果必须为地址传值
	if condition.DbName == "" || condition.TblName == "" {
		err = fmt.Errorf("MongoStore::FindAll function parameter error, not about table name[parameter:%+v]", condition)
		return &reply, err
	}

	filter := agent.ParseFilter(condition)
	options := agent.ParseManyOptions(condition)
	var results []bson.M

	// mongo.S().FindScan(context.TODO(), condition.TblName, condition.DbName, filter, &results, options)
	if err = mongo.S().FindAll(condition.TblName, condition.DbName, filter, &results, options); err == nil {
		reply.Data = string(dtype.ToJson(results))
	}

	return &reply, err
}

// Set 同步更新mongoDB
func (m *MongoStore) Set(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	var err error = nil
	reply := protos.StoreResponse{}
	condition := agent.QueryPara{}

	if err := json.Unmarshal([]byte(req.Condition), &condition); err != nil {
		return &reply, err
	}

	defer func() { //捕获异常
		if e := recover(); e != nil {
			err = fmt.Errorf("MongoStore::FindAll panic:%+v", e)
		}
	}()

	if condition.DbName == "" || condition.TblName == "" {
		err = fmt.Errorf("MongoStore::FindAll function parameter error, not about table name[parameter:%+v]", condition)
		return &reply, err
	}

	collection := mongo.S().C(condition.TblName, condition.DbName)
	if collection == nil { //如果database跟table的collection不存在，则返回错误
		err = fmt.Errorf("MongoStore::Del error, table [%s.%s] not exist", condition.DbName, condition.TblName)
	} else {
		filter := agent.ParseFilter(condition)
		var updateVal interface{}
		json.Unmarshal([]byte(req.Data), &updateVal)

		updateData := bson.M{"$set": updateVal}
		//更新数据，没有则插入，批量更新，bulkWrite只是可以分批执行命令，底层还是用基本的updateMany/updateOne/deleteMany/deleteOne/insertOne/insertMany,
		_, err = collection.UpdateMany(context.TODO(), filter, updateData, options.Update().SetUpsert(true))
	}

	return &reply, err
}

// Del 删除mongoDB
func (m *MongoStore) Del(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	var err error = nil
	reply := protos.StoreResponse{}
	condition := agent.QueryPara{}

	if err := json.Unmarshal([]byte(req.Condition), &condition); err != nil {
		return &reply, err
	}

	defer func() { //捕获异常
		if e := recover(); e != nil {
			err = fmt.Errorf("MongoStore::FindAll panic:%+v", e)
		}
	}()

	if condition.DbName == "" || condition.TblName == "" {
		err = fmt.Errorf("MongoStore::Del function parameter error, not about table name[parameter:%+v]", condition)
		return &reply, err
	}

	collection := mongo.S().C(condition.TblName, condition.DbName)
	if collection == nil { //如果database跟table的collection不存在，则返回错误
		err = fmt.Errorf("MongoStore::Del error, table [%s.%s] not exist", condition.DbName, condition.TblName)
	} else {
		filter := agent.ParseFilter(condition)
		_, err = collection.DeleteMany(context.TODO(), filter)
	}

	return &reply, err
}

// Add 插入mongoDB
func (m *MongoStore) Add(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	var err error = nil
	reply := protos.StoreResponse{}
	condition := agent.QueryPara{}

	if err := json.Unmarshal([]byte(req.Condition), &condition); err != nil {
		return &reply, err
	}

	defer func() { //捕获异常
		if e := recover(); e != nil {
			err = fmt.Errorf("MongoStore::Add panic:%+v", e)
		}
	}()

	if condition.DbName == "" || condition.TblName == "" {
		err = fmt.Errorf("MongoStore::Add function parameter error, not about table name[parameter:%+v]", condition)
		return &reply, err
	}

	collection := mongo.S().C(condition.TblName, condition.DbName)
	if collection == nil { //如果database跟table的collection不存在，则返回错误
		err = fmt.Errorf("MongoStore::Add error, table [%s.%s] not exist", condition.DbName, condition.TblName)
	} else {
		var addData interface{}
		json.Unmarshal([]byte(req.Data), &addData)
		_, err = collection.InsertOne(context.TODO(), addData)
	}

	return &reply, err
}

// PublishMsg 发布异步消息
func (m *MongoStore) PublishMsg(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	var err error = nil
	reply := protos.StoreResponse{}

	defer func() { //捕获异常
		if e := recover(); e != nil {
			err = fmt.Errorf("MongoStore::PublishMsg panic:%+v", e)
		}
	}()

	var msg agent.AsyncMsg
	if err := json.Unmarshal([]byte(req.Condition), &msg); err != nil {
		return &reply, err
	}
	mq.S().PublishMsg(msg)
	return &reply, err
}

// GetRedis 仅从redis获取 类型string
func (r *RedisStore) Do(ctx context.Context, req *protos.RedisStoreRequest) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}
	var cmd []interface{}
	if err := json.Unmarshal([]byte(req.Cmd), &cmd); err != nil {
		return &reply, err
	}

	result, err := redis.S().Do(cmd...)
	if err == nil {
		reply.Data = string(dtype.ToJson(result))
	}

	return &reply, err
}

func (e *Empty) AfterInit() {
	//本地cache
	//memory.NewMemoryStore()
	// 连接Redis
	if err := redis.S().Connect(); err != nil {
		logger.Log.Errorf("连接Redis出错: %s", err.Error())
	}
	// 连接MongoDB
	if err := mongo.S().Connect(); err != nil {
		logger.Log.Errorf("连接MongoDB出错: %s", err.Error())
	}
	//连接nats-streaming
	if err := mq.S().Connect(); err != nil {
		logger.Log.Errorf("连接nats-streaming出错: %s", err.Error())
	}
}
func (e *Empty) BeforeShutdown() {
	// 关闭Redis
	if err := redis.S().Disconnect(); err != nil {
		logger.Log.Errorf("关闭Redis连接出错: %s", err.Error())
	}

	// 关闭MongoDB连接
	if err := mongo.S().Disconnect(); err != nil {
		logger.Log.Errorf("关闭MongoDB连接出错: %s", err.Error())
	}
	// 关闭nats-streaming连接
	if err := mq.S().Disconnect(); err != nil {
		logger.Log.Errorf("关闭MongoDB连接出错: %s", err.Error())
	}
}
