package mq

const (
	KafkaProducer = 0 //生产者
	KafkaConsumer = 1 //消费者
)

//kafka实例
type Kafka struct {
	ID          uint     //身份，0生产者 1消费者
	HostCluster []string //集群连接信息
	Topic       []string //topic
	Sync        bool     //是否同步
	GroupID     string   //groud id,消费者消费使用
}

// var (
// 	single *NatsStreaming
// 	once   sync.Once
// )

// //S 单例
// func S() *Kafka {
// 	once.Do(func() {
// 		single = &Kafka{}
// 	})
// 	return single
// }

//连接
// func (kf *Kafka) Connect() (err error) {
// 	var cfg *config.Config = pitaya.GetConfig()
// 	if cfg != nil && cfg.GetString("dataapp.mq.kafka.host") != "" {
// 		kf.ID = uint(cfg.GetInt("dataapp.mq.kafka.type"))
// 		kf.HostCluster = cfg.GetStringSlice("dataapp.mq.kafka.host")
// 		kf.Sync = cfg.GetBool("dataapp.mq.kafka.sync")
// 		kf.Topic = cfg.GetStringSlice("dataapp.mq.kafka.topic")
// 		kf.GroupID = cfg.GetString("dataapp.mq.kafka.groud_id")
// 	}

// 	if kf.ID == KafkaProducer { //生产者
// 		return kf.ProducerReconn()
// 	} else { //消费者
// 		return kf.ConsumerReconn()
// 	}
// }

// func (kf *Kafka) Close() error {
// 	var err error = nil
// 	if kf.SyncProducer != nil {
// 		err = kf.SyncProducer.Close()
// 	}
// 	if kf.AsyncProducer != nil {
// 		err = kf.AsyncProducer.Close()
// 	}
// 	return err
// }

// //生产者重连
// func (kf *Kafka) ProducerReconn() error {
// 	needConn := false
// 	var err error = nil
// 	if kf.Sync && kf.SyncProducer == nil {
// 		needConn = true
// 	} else if kf.AsyncProducer == nil {
// 		needConn = true
// 	}
// 	defer func() { //捕获异常
// 		if e := recover(); e != nil {
// 			kf.Close()
// 			kf.Connect()
// 		}
// 	}()
// 	if needConn {
// 		config := sarama.NewConfig()
// 		config.Producer.Timeout = 5 * time.Second
// 		config.Producer.Partitioner = sarama.NewRandomPartitioner //随机的分区类型
// 		loop := 0
// 		for loop < ReconnTimes {
// 			if kf.Sync { //同步
// 				config.Producer.RequiredAcks = sarama.WaitForAll //等待服务器所有副本都保存成功后的响应,确保消息的保存
// 				//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
// 				config.Producer.Return.Successes = true
// 				config.Producer.Return.Errors = true
// 				kf.SyncProducer, err = sarama.NewSyncProducer(kf.HostCluster, config)
// 				if err == nil {
// 					break
// 				}
// 			} else { //异步
// 				kf.AsyncProducer, err = sarama.NewAsyncProducer(kf.HostCluster, config)
// 				if err == nil {
// 					break
// 				}
// 			}
// 			loop++
// 		}
// 	}
// 	return err
// }

// //消费者重连
// func (kf *Kafka) ConsumerReconn() error {
// 	var err error = nil
// 	if kf.Consumer == nil {
// 		loop := 0
// 		config := cluster.NewConfig()
// 		config.Group.Return.Notifications = false
// 		config.Consumer.Offsets.CommitInterval = 1 * time.Second
// 		for loop < ReconnTimes {
// 			//不能使用sarama.NewConsumer,进程重启后只能从offset为0的或重启后再生产的消息，
// 			//不会从上一次重启的offset读取消息，sarama.cluster则会从上一次的offset再次获取
// 			kf.Consumer, err = cluster.NewConsumer(kf.HostCluster, kf.GroupID, kf.Topic, config)
// 			if err == nil {
// 				break
// 			}
// 			loop++
// 		}
// 	}
// 	return err
// }

// //生产者生产消息
// func (kf *Kafka) ProductionMsg(msg AsyncMsg) error {
// 	var err error = nil
// 	if err = kf.Connect(); err == nil {
// 		if kf.ID == KafkaProducer {
// 			defer func() { //捕获异步
// 				if e := recover(); e != nil {
// 					kf.Close()
// 					kf.Connect()
// 				}
// 			}()

// 			kMsg := &sarama.ProducerMessage{}
// 			if msg.Topic != "" {
// 				kMsg.Topic = msg.Topic
// 			} else if len(kf.Topic) > 0 { //没有topic则默认配置
// 				kMsg.Topic = kf.Topic[0]
// 			}
// 			jByte := []byte{}
// 			jByte, err = bson.Marshal(msg)
// 			if err == nil {
// 				kMsg.Value = sarama.ByteEncoder(jByte) //将interface转成json string
// 				var partition int32
// 				var offset int64
// 				loop := 0
// 				for loop < ReconnTimes {
// 					if kf.Sync { //同步
// 						partition, offset, err = kf.SyncProducer.SendMessage(kMsg)
// 						fmt.Printf("KafkaHandler::ProductionMsg result[partition:%d, offset:%d, err:%+v\n]", partition, offset, err)
// 					} else { //异步，交给sarama的异步,不需要等待结果
// 						kf.AsyncProducer.Input() <- kMsg
// 					}
// 					if err == nil {
// 						break
// 					}
// 					loop++
// 				}
// 			}

// 		} else {
// 			return fmt.Errorf("KafkaHandler::ProductionMsg error, not the producer type.....")
// 		}
// 	}
// 	return err
// }

// //消费者消费消息
// func (kf *Kafka) ConsumeStart(consumeFunc func(kMsg *AsyncMsg) error) error {
// 	var err error = nil
// 	//必须设置是消费者身份
// 	if kf.ID == KafkaConsumer {
// 		if kf.Connect() == nil {
// 			//捕获异常
// 			defer func() {
// 				if e := recover(); e != nil {
// 					kf.Close()
// 					kf.Connect()
// 				}
// 			}()
// 			for msg := range kf.Consumer.Messages() {
// 				val := &AsyncMsg{}
// 				if err = bson.Unmarshal(msg.Value, val); err == nil {
// 					fmt.Printf("KafkaHandler::ConsumeMsg json convert result:%+v\n", val)
// 					consumeFunc(val)
// 				}
// 				fmt.Printf("KafkaHandler::ConsumeMsg result:%+v\n", msg)
// 				kf.Consumer.MarkOffset(msg, "") //并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset，帮需要实时改变offset
// 			}
// 		}
// 	} else {
// 		return fmt.Errorf("KafkaHandler::ConsumeMsg error, not the consumer type.....")
// 	}
// 	return err
// }
