package redis

import (
	"dataapp/util/dtype"
	"fmt"
	"testing"
)

func TestStore(t *testing.T) {
	S().Connect()
	var v interface{}
	// jsonVal, _ := json.Marshal(data)
	// obj := &RegionModel{}
	// json.Unmarshal(jsonVal, obj)
	//S().HSetStruct("game_dev22.tbl_region.203", data)

	// S().Do("hset", "game_dev22.tbl_region.203", "ID", 23)
	// S().Do("hset", "game_dev22.tbl_region.203", "HeadURL", "asdasdfasdf")

	// v, err := S().Do("hmget", "game_dev22.tbl_region.203", "ID", "HeadURL")
	// fmt.Printf("all values=%s err_hgetall=%v\n", string(dtype.ToJson(v)), err)
	// v, err := S().client.HGet(context.TODO(), "game_dev22.tbl_region.203", "ID").Result()

	// v, err := S().Do("hgetall", "game_dev22.tbl_region.203")
	v, err := S().Do("get", "foo")

	fmt.Printf("all values=%s err_hgetall=%v\n", string(dtype.ToJson(v)), err)
	defer S().Disconnect()
}
