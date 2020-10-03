package memtable

import "github.com/PatchouliG/wisckey-db/record"

// not thread safe
type Memtable struct {
	m map[record.Key]record.Record
}

func NewMemtable() *Memtable {
	return &Memtable{m: make(map[record.Key]record.Record)}
}

func (mt *Memtable) Put(r record.Record) {
	mt.m[r.Key()] = r
}

// false if not found
func (mt *Memtable) Get(key record.Key) (res record.Record, ok bool) {
	res, ok = mt.m[key]
	return
}

// false if not found
func (mt *Memtable) Delete(key record.Key) bool {
	_, ok := mt.m[key]
	if !ok {
		return false
	}
	delete(mt.m, key)
	return true
}
