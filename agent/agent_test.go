package agent

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/topfreegames/pitaya/client"
)

var clients []*client.Client
var update = flag.Bool("update", true, "update server binary")

func getClients(n, port int) []*client.Client {
	c := make([]*client.Client, n)
	for i := 0; i < n; i++ {
		c[i] = client.New(logrus.FatalLevel)
		err := c[i].ConnectTo(fmt.Sprintf("%s:%d", "localhost", port))
		if err != nil {
			panic(err)
		}
	}
	return c

}

func TestMain(m *testing.M) {
	flag.Parse()
	if *update {
		cmd := exec.Command("go", "build", "-o", "../examples/testing/server", "../main.go")
		err := cmd.Run()
		if err != nil {
			panic(err)
		}
	}
	exit := m.Run()
	os.Exit(exit)
}

func TestMongoGet(t *testing.T) {
	c := client.New(logrus.FatalLevel)
	if err := c.ConnectTo(fmt.Sprintf("%s:%d", "localhost", 32222)); err != nil {
		panic(err)
	}

	_, err := c.SendRequest("connector.connector.get", nil)
	if err != nil {
		panic(err)
	}
}

func BenchmarkCreateClient(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := client.New(logrus.FatalLevel)
		err := g.ConnectTo(fmt.Sprintf("%s:%d", "localhost", 32222))
		defer g.Disconnect()
		if err != nil {
			b.Logf("failed to connect")
			b.FailNow()
		}
	}

}

func BenchmarkMongoGet(b *testing.B) {
	clients := getClients(1, 32222)
	b.ResetTimer()
	b.N = 10000
	for i := 0; i < b.N; i++ {
		_, err := clients[0].SendRequest("connector.connector.get", nil)
		if err != nil {
			b.Logf("failed to send request to server")
			b.FailNow()
		}
		<-clients[0].IncomingMsgChan
	}
}

// func TestUserRPC(t *testing.T) {
// 	port1 := helpers.GetFreePort(t)
// 	fmt.Printf("localhost:%d\n", port1)

// 	sdPrefix := fmt.Sprintf("%s/", uuid.New().String())
// 	// set lazy connections
// 	defer helpers.StartServer(t, false, true, "dataapp", 0, sdPrefix, false, true)()
// 	defer helpers.StartServer(t, true, true, "connector", port1, sdPrefix, false, false)()
// 	c1 := client.New(logrus.InfoLevel)

// 	err := c1.ConnectTo(fmt.Sprintf("localhost:%d", port1))
// 	fmt.Printf("localhost:%d\n", port1)

// 	assert.NoError(t, err)
// 	defer c1.Disconnect()

// 	tables := []struct {
// 		name  string
// 		route string
// 		data  []byte
// 		res   string
// 	}{
// 		{"select_mongo", "connector.connector.get", []byte(`{"route":"dataapp.mongostore.get","data":"thisthis"}`), `{"data":"got thisthis"}`},
// 		{"redisdo", "connector.connector.redisdo", []byte(`{"route":"dataapp.mongostore.get","data":"thisthis"}`), `{"data":"got thisthis"}`},
// 		// {"dataapp_get", "dataapp.store.get", []byte(`{"route":"connector.testremotesvc.rpctestrawptrreturnsptr","data":"thisthis"}`), `{"code":200,"msg":"got thisthis"}`},

// 	}

// 	for _, table := range tables {
// 		t.Run(table.name, func(t *testing.T) {
// 			_, err := c1.SendRequest(table.route, table.data)
// 			assert.NoError(t, err)
// 			msg := helpers.ShouldEventuallyReceive(t, c1.IncomingMsgChan).(*message.Message)
// 			assert.Regexp(t, table.res, string(msg.Data))
// 		})
// 	}
// }
