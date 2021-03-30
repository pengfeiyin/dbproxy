package agent

import (
	"context"
	"dataapp/protos"
	"dataapp/util/dtype"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/topfreegames/pitaya"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Selector 查找的条件
type Filter struct {
	Val  interface{} //数据
	Flag int         //标志，等于，不等于，小于，不小于，大于，不大于，在其中，不在其中
}

//条件查找的标识
const (
	SELECTOR = iota
	GT       //大于
	GTE      //大于等于
	LT       //小于
	LTE      //小于等于
	NE       //不等于
	IN       //在[]中
	NIN      //不在[]中
)

//
const (
	DESC = -1 //降序
	ASC  = 1  //升序
)

//QueryPara 查找的参数
type QueryPara struct {
	DbName     string             //数据库名
	TblName    string             //表格名
	PrimaryKey string             //主键值
	Field      string             //field名字
	Filter     map[string]*Filter //条件查找
	Sort       map[string]int32   //排序数据，key:字段名， value:-1倒序，1升序
	Limit      int32              //限制返回的数量
	Skip       int32              //设置skip
}

const (
	DB_OPT_TYPE = iota
	GET         //获取
	SET         //设置
	DEL         //删除
	INS         //插入
)

//同步的消息格式
type AsyncMsg struct {
	Topic   string      //topic
	Query   QueryPara   //查找的参数
	OptType uint        //操作类型
	Val     interface{} //保存的数据
}

//MongoGet mongoDB操作查询
func MongoGet(para QueryPara, result interface{}) (err error) {
	//参数校验，返回的结果必须为地址传值
	if para.DbName == "" || para.TblName == "" {
		err = fmt.Errorf("MongoGet function parameter error, not about table name[%s.%s]", para.DbName, para.TblName)
		return
	}
	//必须为址传递
	if reflect.TypeOf(result).Kind() != reflect.Ptr {
		err = fmt.Errorf("MongoGet function error, result parameter not pointer:%+v", result)
		return
	}

	req := protos.MongoStoreRequest{Condition: string(dtype.ToJson(&para))}
	reply := protos.StoreResponse{}

	refVal := reflect.ValueOf(result)
	if refVal.Elem().Kind() == reflect.Array || refVal.Elem().Kind() == reflect.Slice { //查找多个
		err = pitaya.RPC(context.TODO(), "dataapp.mongostore.findall", &reply, &req)
	} else {
		err = pitaya.RPC(context.TODO(), "dataapp.mongostore.findone", &reply, &req)
	}
	json.Unmarshal([]byte(reply.Data), result)
	return
}

//MongoSet 同步更新mongoDB操作
func MongoSet(para QueryPara, dataVal interface{}) (err error) {
	reply := protos.StoreResponse{}

	//参数校验，返回的结果必须为地址传值
	if para.DbName == "" || para.TblName == "" {
		err = fmt.Errorf("MongoSetSync function parameter error, not about table name[%s.%s]", para.DbName, para.TblName)
		return
	}
	req := protos.MongoStoreRequest{
		Condition: string(dtype.ToJson(&para)),
		Data:      string(dtype.ToJson(&dataVal)),
	}
	err = pitaya.RPC(context.TODO(), "dataapp.mongostore.set", &reply, &req)
	return
}

//MongoGet 异步删除mongoDB操作
func MongoDel(para QueryPara) (err error) {
	reply := protos.StoreResponse{}

	//参数校验，返回的结果必须为地址传值
	if para.DbName == "" || para.TblName == "" {
		err = fmt.Errorf("MongoDel function parameter error, not about table name[%s.%s]", para.DbName, para.TblName)
		return
	}
	req := protos.MongoStoreRequest{Condition: string(dtype.ToJson(&para))}

	err = pitaya.RPC(context.TODO(), "dataapp.mongostore.del", &reply, &req)
	return
}

//MongoAdd 异步插入mongoDB操作
func MongoAdd(para QueryPara, dataVal []interface{}) (err error) {
	reply := protos.StoreResponse{}

	//参数校验，返回的结果必须为地址传值
	if para.DbName == "" || para.TblName == "" {
		err = fmt.Errorf("MongoAdd function parameter error, not about table name[%s.%s]", para.DbName, para.TblName)
		return
	}

	req := protos.MongoStoreRequest{
		Condition: string(dtype.ToJson(&para)),
		Data:      string(dtype.ToJson(&dataVal)),
	}

	err = pitaya.RPC(context.TODO(), "dataapp.mongostore.add", &reply, &req)
	return
}

//PublishMsg 发布异步消息
func PublishMsg(opt uint, para QueryPara, val interface{}) (err error) {
	//参数校验，返回的结果必须为地址传值
	if para.DbName == "" || para.TblName == "" {
		err = fmt.Errorf("PublishMsg function parameter error, not about table name[%s.%s]", para.DbName, para.TblName)
		return
	}
	msg := AsyncMsg{
		OptType: opt,
		Val:     val,
		Query:   para,
	}
	req := protos.MongoStoreRequest{Condition: string(dtype.ToJson(&msg))}

	reply := protos.StoreResponse{}
	err = pitaya.RPC(context.TODO(), "dataapp.mongostore.publishmsg", &reply, &req)
	return
}

//DoRedis 操作redis
func RedisDo(args ...interface{}) (result interface{}, err error) {
	v, _ := json.Marshal(args)
	req := protos.RedisStoreRequest{
		Cmd: string(v),
	}
	reply := protos.StoreResponse{}

	if err := pitaya.RPC(context.TODO(), "dataapp.redisstore.do", &reply, &req); err == nil {
		json.Unmarshal([]byte(reply.Data), &result)
	}
	return result, err
}

//ParseFilter 将参数解析成Mongodb的select语句
func ParseFilter(para QueryPara) bson.M {
	filter := bson.M{}
	for k, v := range para.Filter {
		switch v.Flag {
		case GT:
			filter[k] = bson.M{"$gt": v.Val}
		case GTE:
			filter[k] = bson.M{"$gte": v.Val}
		case LT:
			filter[k] = bson.M{"$lt": v.Val}
		case LTE:
			filter[k] = bson.M{"$lte": v.Val}
		case NE:
			filter[k] = bson.M{"$ne": v.Val}
		case NIN:
			filter[k] = bson.M{"$nin": v.Val}
		case IN:
			filter[k] = bson.M{"$in": v.Val}
		default:
			filter[k] = v.Val
		}
	}
	return filter
}

//解析查找器
func ParseManyOptions(para QueryPara) *options.FindOptions {
	findOptions := options.Find()
	for k, v := range para.Sort { //排序设置
		findOptions.SetSort(bson.M{k: v})
	}
	findOptions.SetSkip(int64(para.Skip)).SetLimit(int64(para.Limit))
	return findOptions
}

//解析查找器
func ParseOneOptions(para QueryPara) *options.FindOneOptions {
	findOptions := options.FindOne()
	for k, v := range para.Sort { //排序设置
		findOptions.SetSort(bson.M{k: v})
	}
	return findOptions
}
