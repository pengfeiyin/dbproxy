# dataapp

基于 [pitaya](https://github.com/topfreegames/pitaya) 实现的游戏数据层访问代理RPC服务

## 简介
业务层访问数据时，通过dataapp服务提供的RPC接口访问缓存数据和db数据。缓存目前支持redis，DB目前支持mongo，支持异步写DB数据。

dataapp服务支持redis单机模式、集群模式、哨兵模式

dataapp服务mq采用nats-streaming。支持单机模式、集群模式。

dataapp服务db采用mongo。仅支持单机模式


主要SDK功能:

- 同步读取Mongo func MongoGet(para QueryPara, result interface{}) (err error)
- 同步更新Mongo func MongoSet(para QueryPara, dataVal interface{}) (err error) 
- 同步删除Mongo func MongoDel(para QueryPara) (err error) 
- 同步插入Mongo func MongoAdd(para QueryPara, dataVal []interface{}) (err error) 
- 异步写Mongo func PublishMsg(opt uint, para QueryPara, val interface{}) (err error) 
- redis操作 func RedisDo(args ...interface{}) (result interface{}, err error) 

dataaapp 后端服务启动命令 dataapp -type [dataapp] -port [32222]  -frontend [false] -debug [true]

如测试有需要启动前端服：dataapp -type connector -port 32223  -frontend true -debug true
