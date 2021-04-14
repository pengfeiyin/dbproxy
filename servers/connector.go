package servers

import (
	"context"
	"dataapp/agent"
	"dataapp/protos"
	"dataapp/util/dtype"
	"encoding/json"
	"log"

	"github.com/topfreegames/pitaya/component"
)

// Connector struct
type Connector struct {
	component.Base
}

func (c *Connector) Get(ctx context.Context) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}
	para := agent.QueryPara{
		DbName:  "game_dev22",
		TblName: "tbl_region",
		Filter:  map[string]*agent.Filter{},

		// Limit: 3,
		// Skip:  4,
		// Sort:  map[string]int32{"id": 1},
		// KeyName: "id",
		// Key:     201,
	}
	para.Filter["id"] = &agent.Filter{Val: 201}
	var result interface{}
	err := agent.MongoGet(para, &result)
	reply.Data = string(dtype.ToJson(result))

	// var results []interface{}
	// err := agent.MongoGet(para, &results)
	// reply.Data = string(dtype.ToJson(results))
	// if _, err = redis.S().Client().Ping(context.TODO()).Result(); err != nil {
	// 	logger.Log.Errorf("RedisStore::SingleConnect single error, cann't ping the redis server[error:%+v]", err)
	// } else {
	// 	logger.Log.Debugf("RedisStore::SingleConnect single successfully")
	// }

	// redisKey := fmt.Sprintf("%s.%s.%d", para.DbName, para.TblName, 203)
	// redis.S().Do("set", redisKey, 2)
	log.Println(reply.Data)

	return &reply, err
}

func (c *Connector) RedisDo(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}

	result, err := agent.RedisDo("hgetall", "game_dev22.tbl_region.203")

	if err == nil {
		var obj interface{}
		json.Unmarshal(dtype.ToJson(result), &obj)
		reply.Data = string(dtype.ToJson(obj))
	}

	return &reply, err
}

func (c *Connector) Set(ctx context.Context, req *protos.MongoStoreRequest) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}
	para := agent.QueryPara{
		DbName:  "game_dev22",
		TblName: "tbl_region",
		Filter:  map[string]*agent.Filter{},
	}

	para.Filter["id"] = &agent.Filter{Val: 201}
	err := agent.MongoSet(para, nil)
	return &reply, err
}

func (c *Connector) Del(ctx context.Context) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}
	para := agent.QueryPara{
		DbName:  "game_dev22",
		TblName: "tbl_region",
		Filter:  map[string]*agent.Filter{},
	}
	para.Filter["id"] = &agent.Filter{Val: 201}

	err := agent.MongoDel(para)
	return &reply, err
}

func (c *Connector) Add(ctx context.Context) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}
	para := agent.QueryPara{
		DbName:  "game_dev22",
		TblName: "tbl_region",
	}

	err := agent.MongoAdd(para, nil)
	return &reply, err
}

func (c *Connector) PublishMsg(ctx context.Context) (*protos.StoreResponse, error) {
	reply := protos.StoreResponse{}
	para := agent.QueryPara{
		DbName:  "game_dev22",
		TblName: "tbl_region",
		Filter:  map[string]*agent.Filter{},
	}
	para.Filter["id"] = &agent.Filter{Val: 203}

	err := agent.PublishMsg(agent.DEL, para, nil)
	return &reply, err
}
