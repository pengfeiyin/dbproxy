package memory

import (
	"dataapp/util/dtype"
	"errors"
	"sync"
	"time"
)

//MemoryStore 内存数据存储
type MemoryStore struct {
	sync.RWMutex
	values map[string]*Record
}

//List memory
func (ms *MemoryStore) List() ([]*Record, error) {
	ms.RLock()
	defer ms.RUnlock()

	var values []*Record

	for _, v := range ms.values {
		if v.CheckState() {
			values = append(values, v)
		}
	}

	return values, nil
}

func (ms *MemoryStore) Read(keys ...string) ([]*Record, error) {
	ms.RLock()
	defer ms.RUnlock()

	var records []*Record

	for _, key := range keys {
		v, ok := ms.values[key]
		if !ok {
			return nil, errors.New("NotFound")
		}

		if !v.CheckState() {
			return nil, errors.New("NotFound")
		}

		records = append(records, v)
	}

	return records, nil
}

func (ms *MemoryStore) Write(records ...*Record) error {
	ms.Lock()
	defer ms.Unlock()

	for _, r := range records {
		ms.values[r.key] = r
	}

	return nil
}

//Delete 删除
func (ms *MemoryStore) Delete(keys ...string) error {
	ms.Lock()
	defer ms.Unlock()

	for _, key := range keys {
		delete(ms.values, key)
	}

	return nil
}

//Get 获取
func (ms *MemoryStore) Get(key string) (interface{}, error) {
	ms.RLock()
	defer ms.RUnlock()

	v, ok := ms.values[key]
	if !ok {
		return nil, errors.New("NotFound")
	}

	return v.Value(), nil
}

//Set 设置
func (ms *MemoryStore) Set(key string, value interface{}, expiry ...time.Duration) error {
	return ms.Write(NewRecord(key, value, expiry...))
}

//Int 获取Int
func (ms *MemoryStore) Int(key string) (int, error) {
	val, err := ms.Get(key)
	if err != nil {
		return 0, err
	}

	return dtype.ParseInt(val), nil
}

//Int32 获取Int32
func (ms *MemoryStore) Int32(key string) (int32, error) {
	val, err := ms.Get(key)
	if err != nil {
		return int32(0), err
	}
	return dtype.ParseInt32(val), nil
}

//Int64 获取Int64
func (ms *MemoryStore) Int64(key string) (int64, error) {
	val, err := ms.Get(key)
	if err != nil {
		return 0, err
	}
	return dtype.ParseInt64(val), nil
}

//Uint32 Uint32
func (ms *MemoryStore) Uint32(key string) (uint32, error) {
	val, err := ms.Get(key)
	if err != nil {
		return 0, err
	}
	return dtype.ParseUint32(val), nil
}

//Uint64 Uint64
func (ms *MemoryStore) Uint64(key string) (uint64, error) {
	val, err := ms.Get(key)
	if err != nil {
		return 0, err
	}
	return dtype.ParseUint64(val), nil
}

//Bool Bool
func (ms *MemoryStore) Bool(key string) (bool, error) {
	val, err := ms.Get(key)
	if err != nil {
		return false, err
	}
	return dtype.ParseBool(val), nil
}

//NewMemoryStore 新建
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		values: make(map[string]*Record),
	}
}
