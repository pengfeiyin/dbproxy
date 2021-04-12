// Package mongo 连接
// * 查找单个文档时, 如果未找到文件, 则会返回 ErrNoDocuments 错误
// * 查找多个文档时, 如果未找到任何文档, 则会返回 ErrNilDocument 错误
// * bson.M 是无序的 doc 描述
// * bson.D 是有序的 doc 描述
// * bsonx.Doc 是类型安全的 doc 描述
package mongo

import (
	"context"
	"dataapp/util/dtype"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/topfreegames/pitaya"
	"github.com/topfreegames/pitaya/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	single *MongoStore
	once   sync.Once
)

//MongoStore 数据存储
type MongoStore struct {
	opts   *Options
	ctx    context.Context
	cancel context.CancelFunc
	client *mongo.Client
}

//S 单例
func S() *MongoStore {
	once.Do(func() {
		single = NewMongoStore(
			WithMinPoolSize(5),
			WithMaxPoolSize(10),
			WithURL("mongodb://localhost:27017/"),
		)
	})
	return single
}

//Init MongoStore
func (m *MongoStore) Init(opts ...Option) {
	for _, o := range opts {
		o(m.opts)
	}
}

//DbName MongoStore
func (m *MongoStore) DbName() string {
	return m.opts.DbName
}

//Opts MongoStore
func (m *MongoStore) Opts() *Options {
	return m.opts
}

//Connect MongoStore
func (m *MongoStore) Connect() error {
	if m.client != nil {
		return nil
	}
	var cfg *config.Config = pitaya.GetConfig()

	if cfg != nil {
		m.opts.RawURL = cfg.GetString("dataapp.store.mongo.host")
	}

	if m.opts.RawURL == "" {
		m.opts.RawURL = "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs1"
	} else if !strings.HasPrefix(m.opts.RawURL, "mongodb://") {
		m.opts.RawURL = "mongodb://" + m.opts.RawURL
	}

	opts := options.Client().
		SetMinPoolSize(m.opts.MinPoolSize).
		SetMaxPoolSize(m.opts.MaxPoolSize).
		SetConnectTimeout(m.opts.ConnectTimeout).
		SetSocketTimeout(m.opts.SocketTimeout).
		SetMaxConnIdleTime(m.opts.MaxConnIdleTime).
		SetRetryWrites(true).
		SetRetryReads(true).
		ApplyURI(m.opts.RawURL)

	// if opts.ReplicaSet == nil || *opts.ReplicaSet == "" {
	// 	return errors.New("this system only supports replica sets. example: mongodb://0.0.0.0:27017,0.0.0.0:27018,0.0.0.0:27019/?replicaSet=rs1")
	// }

	m.ctx, m.cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer m.cancel()
	if mc, err := mongo.Connect(m.ctx, opts); err != nil {
		return err
	} else {
		m.client = mc
	}

	// 检查MongoDB连接
	if err := m.client.Ping(m.ctx, readpref.Primary()); err != nil {
		return err
	}

	return nil
}

//Disconnect MongoStore
func (m *MongoStore) Disconnect() error {
	if m.client == nil {
		return nil
	}

	if err := m.client.Disconnect(m.ctx); err != nil {
		return err
	}

	m.client = nil

	return nil
}

// Client 获取客户端
func (m *MongoStore) Client() *mongo.Client {
	return m.client
}

//D Database 获取数据库对象
func (m *MongoStore) D(dbname ...string) *mongo.Database {
	if len(dbname) > 0 && dbname[0] != "" {
		return m.client.Database(dbname[0])
	}
	return m.client.Database(m.opts.DbName)
}

//C Collection 获取集合对象
func (m *MongoStore) C(name string, dbname ...string) *mongo.Collection {
	if len(dbname) > 0 && dbname[0] != "" {
		return m.client.Database(dbname[0]).Collection(name)
	}
	return m.client.Database(m.opts.DbName).Collection(name)
}

//CloneC 克隆集合对象
func (m *MongoStore) CloneC(name string, dbname ...string) (*mongo.Collection, error) {
	return m.C(name, dbname...).Clone()
}

//GetIncID id
func (m *MongoStore) GetIncID(id string) (int64, error) {
	return GetIncID(context.Background(), m.D(), id)
}

//ListCollectionNames 获取集合列表
func (m *MongoStore) ListCollectionNames(dbname ...string) ([]string, error) {
	return m.D(dbname...).ListCollectionNames(context.Background(), bson.M{})
}

// SelectOne 通过反射查询单条记录
func (m *MongoStore) SelectOne(table, dbname string, filter interface{}, model reflect.Type, options ...*options.FindOneOptions) (interface{}, error) {
	var res = dtype.Elem(model).Addr().Interface()
	if err := m.FindOne(table, dbname, filter, res, options...); err != nil {
		return nil, err
	}
	return res, nil
}

// SelectAll 通过反射查询多条记录
func (m *MongoStore) SelectAll(table, dbname string, filter interface{}, model reflect.Type, options ...*options.FindOptions) ([]interface{}, error) {
	rows := dtype.SliceElem(model)
	if err := m.FindAll(table, dbname, filter, rows.Addr().Interface(), options...); err != nil {
		return nil, err
	}

	if rows.IsNil() {
		return nil, nil
	}

	var result []interface{}
	for i := 0; i < rows.Len(); i++ {
		result = append(result, rows.Index(i).Interface())
	}

	return result, nil
}

func (m *MongoStore) FindOne(table, dbname string, filter interface{}, result interface{}, options ...*options.FindOneOptions) error {
	col := m.C(table, dbname)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReadWriteTimeout)
	defer cancel()

	if filter == nil {
		filter = bson.M{}
	}

	if err := col.FindOne(ctx, filter, options...).Decode(result); err != nil {
		return err
	}

	return nil
}

func (m *MongoStore) FindAll(table, dbname string, filter interface{}, result interface{}, options ...*options.FindOptions) error {
	col := m.C(table, dbname)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultReadWriteTimeout)
	defer cancel()

	if filter == nil {
		filter = bson.M{}
	}

	cur, err := col.Find(ctx, filter, options...)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	if err := cur.All(context.Background(), result); err != nil {
		return err
	}

	return nil
}

// 分段获取数据
func (m *MongoStore) FindScan(ctx context.Context, table, dbname string, filter interface{}, result interface{}, options ...*options.FindOptions) error {
	if filter == nil {
		filter = bson.M{}
	}
	col := m.C(table, dbname)

	count, err := col.CountDocuments(ctx, filter)
	if err != nil {
		return err
	}

	if count > 0 {
		cur, err := col.Find(ctx, filter, options...)
		if err != nil {
			return err
		}
		defer cur.Close(ctx)

		if err := cur.All(ctx, result); err != nil {
			return err
		}
	}

	return nil
}

//NewMongoStore MongoStore
func NewMongoStore(opts ...Option) *MongoStore {
	ms := &MongoStore{
		opts: newOptions(opts...),
	}
	return ms
}
