package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
)

type RecordWithTransaction struct {
	record.Record
	snapshot.Id
}

func NewRecordWithTransaction(r record.Record, id snapshot.Id) RecordWithTransaction {
	return RecordWithTransaction{r, id}
}

// not thread safe
type Memtable struct {
	m map[record.Key][]RecordWithTransaction
}

func NewMemtable() *Memtable {
	return &Memtable{m: make(map[record.Key][]RecordWithTransaction)}
}

func (mt *Memtable) Put(rt RecordWithTransaction) {
	if rs, ok := mt.m[rt.Key()]; ok {
		mt.m[rt.Key()] = append(rs, rt)
	} else {
		mt.m[rt.Key()] = []RecordWithTransaction{rt}
	}
}

// false if not found
// return latest record
func (mt *Memtable) Get(key record.Key) (RecordWithTransaction, bool) {
	rs, ok := mt.m[key]
	if !ok {
		return RecordWithTransaction{}, false
	}
	res := rs[len(rs)-1]
	if res.IsDeleted() {
		return RecordWithTransaction{}, false
	}
	return res, true
}

func (mt *Memtable) GetWithSnapshot(key record.Key, id snapshot.Id) (res RecordWithTransaction, found bool) {
	rs, ok := mt.m[key]
	if !ok {
		found = false
		return
	}
	for i := len(rs) - 1; i >= 0; i-- {
		r := rs[i]
		if r.Id.Cmp(id) > 0 {
			continue
		}
		if r.IsDeleted() {
			found = false
			return
		}
		return r, true
	}
	found = false
	return
}

// false if not found
func (mt *Memtable) Delete(key record.Key, id snapshot.Id) bool {
	rs, ok := mt.m[key]
	if !ok {
		return false
	}
	mt.m[key] = append(rs, NewRecordWithTransaction(record.NewDeleteRecord(key), id))
	return true
}
