package mongo

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/mongo"
)

// Table Table
type Table struct {
	name    string             // 表名称
	pkField string             // 主键字段名
	fkField string             // 外键字段名
	pkKind  string             // 主键数据类型
	fkKind  string             // 外键数据类型
	model   reflect.Type       // 结构体反射类型
	index   []mongo.IndexModel // 索引
	data    []interface{}      // 初始化数据
}

//Name  Mongo
func (dt *Table) Name() string {
	return dt.name
}

//PkField Mongo
func (dt *Table) PkField() string {
	return dt.pkField
}

//FkField Mongo
func (dt *Table) FkField() string {
	return dt.fkField
}

//PkKind Mongo
func (dt *Table) PkKind() string {
	return dt.pkKind
}

//FkKind Mongo
func (dt *Table) FkKind() string {
	return dt.fkKind
}

//Index Mongo
func (dt *Table) Index() []mongo.IndexModel {
	return dt.index
}

//Data Mongo
func (dt *Table) Data() []interface{} {
	return dt.data
}

//Model Mongo
func (dt *Table) Model() reflect.Type {
	return dt.model
}

// SetIndex 设置索引
func (dt *Table) SetIndex(index []mongo.IndexModel) {
	dt.index = index
}

// AddIndex 追加索引
func (dt *Table) AddIndex(index []mongo.IndexModel) {
	dt.index = append(dt.index, index...)
}

//SetData  Mongo
func (dt *Table) SetData(data []interface{}) {
	dt.data = data
}

//NewTable Mongo
func NewTable(name, pkField, fkField string, model interface{}) *Table {
	vo := reflect.ValueOf(model)
	mt := &Table{
		name:    name,
		pkField: pkField,
		fkField: fkField,
		index:   nil,
		data:    nil,
		model:   vo.Type(),
	}

	// 获取主键/外键值数据类型
	if pkField != "" || fkField != "" {
		var pkKind, fkKind string
		for i := 0; i < vo.NumField(); i++ {
			tf := vo.Type().Field(i)
			field := tf.Tag.Get("bson")

			// 获取主键数据类型
			if field == pkField {
				pkKind = tf.Type.String()
			}
			// 获取外键数据类型
			if fkField != "" && field == fkField {
				fkKind = tf.Type.String()
			}
		}

		if pkField != "" && pkKind == "" {
			panic(fmt.Sprintf("[%s][%s] the primary key field was not found in the structure", name, pkField))
		}
		if fkField != "" && fkKind == "" {
			panic(fmt.Sprintf("[%s][%s] a foreign key was set, but no foreign key field was found in the structure", name, fkField))
		}

		mt.pkKind = pkKind
		mt.fkKind = fkKind
	}

	return mt
}
