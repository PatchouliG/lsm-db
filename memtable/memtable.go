package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/transaction"
)

type RecordWithTransaction struct {
	record.Record
	transaction.Id
}

func NewRecordWithTransaction(r record.Record, id transaction.Id) RecordWithTransaction {
	return RecordWithTransaction{r, id}
}

// not thread safe
type Memtable struct {
	m map[record.Key]RecordWithTransaction
}

func NewMemtable() *Memtable {
	return &Memtable{m: make(map[record.Key]RecordWithTransaction)}
}

func (mt *Memtable) Put(r RecordWithTransaction) {
	mt.m[r.Key()] = r
}

// false if not found
func (mt *Memtable) Get(key record.Key) (res RecordWithTransaction, ok bool) {
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

func (mt *Memtable) Clone() *Memtable {
	res := NewMemtable()
	for _, v := range mt.m {
		res.Put(v)
	}
	return res
}
