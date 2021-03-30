package mongo

//Opts
var (
	Opts = &struct {
		MongoURL     string // MongoDB Proxy URI地址
		MongoMinPool uint64 // MongoDB 最小连接数
		MongoMaxPool uint64 // MongoDB 最大连接数
	}{}
)
