package mongo

import (
	"fmt"
	"log"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestStore(t *testing.T) {
	S().Connect()

	//获取单条
	result := bson.M{}
	filter := bson.M{"id": 201}

	err := S().FindOne("tbl_region", "game_dev22", filter, result)
	log.Println(result, err)

	//获取全部
	// var results []bson.M
	// err := S().FindAll("tbl_region", "game_dev22", nil, &results, nil)
	// log.Println(results, err)
	// for _, value := range results {
	// 	log.Println(value)
	// }

	//scan
	// var scanResults []interface{}
	// S().FindScan(context.Background(), "tbl_region", "game_dev22", 6, 4, nil, &scanResults)
	// // log.Println(scanResults, err)
	// for _, value := range scanResults {
	// 	log.Println(value)
	// }

	defer S().Disconnect()

	fmt.Printf("MongoDB连接成功\n")
}
