package main

import (
	"dataapp/servers"
	"flag"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/topfreegames/pitaya"
	"github.com/topfreegames/pitaya/acceptor"
	"github.com/topfreegames/pitaya/component"
	"github.com/topfreegames/pitaya/serialize/json"
)

func main() {
	address := flag.String("address", "0.0.0.0", "the address to listen")
	port := flag.Int("port", 32222, "the port to listen")
	svType := flag.String("type", "dataapp", "the server type")
	isFrontend := flag.Bool("frontend", false, "if server is frontend")
	debug := flag.Bool("debug", false, "turn on debug logging")
	// sdPrefix := flag.String("sdprefix", "pitaya/", "prefix to discover other servers")
	// grpc := flag.Bool("grpc", false, "turn on grpc")
	// grpcPort := flag.Int("grpcport", 3434, "the grpc server port")

	flag.Parse()
	defer pitaya.Shutdown()

	l := logrus.New()
	l.Formatter = &logrus.TextFormatter{}
	l.SetLevel(logrus.InfoLevel)
	if *debug {
		l.SetLevel(logrus.DebugLevel)
	}

	pitaya.SetLogger(l)

	config := viper.New()
	config.AddConfigPath(".")
	config.SetConfigName("config")
	err := config.ReadInConfig()
	if err != nil {
		panic(err)
	}

	// config.Set("pitaya.cluster.sd.etcd.prefix", *sdPrefix)
	// config.Set("pitaya.cluster.rpc.server.grpc.address", *address)
	// config.Set("pitaya.cluster.rpc.server.grpc.port", *grpcPort)

	// if *grpc {//
	// 	gs, err := cluster.NewGRPCServer(pitaya.GetConfig(), pitaya.GetServer(), pitaya.GetMetricsReporters())
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	bs := modules.NewETCDBindingStorage(pitaya.GetServer(), pitaya.GetConfig())
	// 	pitaya.RegisterModule(bs, "bindingsStorage")

	// 	gc, err := cluster.NewGRPCClient(
	// 		pitaya.GetConfig(),
	// 		pitaya.GetServer(),
	// 		pitaya.GetMetricsReporters(),
	// 		bs,
	// 		cluster.NewConfigInfoRetriever(pitaya.GetConfig()),
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	pitaya.SetRPCServer(gs)
	// 	pitaya.SetRPCClient(gc)
	// }

	pitaya.SetSerializer(json.NewSerializer())
	if *isFrontend {
		tcp := acceptor.NewTCPAcceptor(fmt.Sprintf("%s:%d", *address, *port))

		pitaya.AddAcceptor(tcp)
		pitaya.Register(&servers.Connector{},
			component.WithName("connector"),
			component.WithNameFunc(strings.ToLower),
		)
	} else {
		pitaya.Register(&servers.Empty{},
			component.WithName("empty"),
			component.WithNameFunc(strings.ToLower),
		)
		pitaya.RegisterRemote(&servers.MongoStore{},
			component.WithName("mongostore"),
			component.WithNameFunc(strings.ToLower),
		)
		pitaya.RegisterRemote(&servers.RedisStore{},
			component.WithName("redisstore"),
			component.WithNameFunc(strings.ToLower),
		)
	}

	pitaya.Configure(*isFrontend, *svType, pitaya.Cluster, map[string]string{}, config)

	pitaya.Start()
}
