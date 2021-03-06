package memory

import "time"

//Record 结构
type Record struct {
	key    string
	value  interface{}
	expiry time.Duration
	time   time.Time
}

//Key memory
func (r *Record) Key() string {
	return r.key
}

//Value memory
func (r *Record) Value() interface{} {
	return r.value
}

//Expiry memory
func (r *Record) Expiry() time.Duration {
	return r.expiry
}

//Time memory
func (r *Record) Time() time.Time {
	return r.time
}

//CheckState memory
func (r *Record) CheckState() bool {
	// expired
	if r.expiry > time.Duration(0) {
		t := time.Since(r.time)

		if t > r.expiry {
			return false
		}
		// update expiry
		r.expiry -= t
		r.time = time.Now()
	}

	return true
}

//NewRecord memory
func NewRecord(key string, value interface{}, expiry ...time.Duration) *Record {
	r := &Record{
		key:   key,
		value: value,
		time:  time.Now(),
	}
	if len(expiry) > 0 {
		r.expiry = expiry[0]
	}
	return r
}
