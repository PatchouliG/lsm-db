package lsm

import (
	"github.com/PatchouliG/lsm-db/memtable"
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
)

// not thread safe
type oneMemtable struct {
	id Id
	// level -> sstable metadata
	levelInfo *levelInfo
	// handle write operation (put delete)
	mutableMemtable *memtable.Memtable
}

func NewOneMemtable() *oneMemtable {
	return &oneMemtable{NextId(), newLevelInfo(), memtable.NewMemtable()}
}

// use old oneMemtable memtable and level info pass
// invalid after call
func (om *oneMemtable) newLsmForCompaction() (*lsmForCompaction, *twoMemtable) {
	lfc := &lsmForCompaction{om.levelInfo.clone(), om.mutableMemtable}
	tm := &twoMemtable{NextId(), om.levelInfo, memtable.NewMemtable(),
		om.mutableMemtable}

	om.levelInfo = nil
	om.mutableMemtable = nil
	return lfc, tm
}

// false if not found or deleted
// todo two memtable may exits
func (om *oneMemtable) Get(key record.Key) (record.Record, bool) {
	if res, ok := om.mutableMemtable.Get(key); ok {
		return res.Record, ok
	}
	return om.levelInfo.get(key)
}

// work for put and delete
// false if memtable write fail(log file size reaches limit)

// own by routine and metadata
func (om *oneMemtable) PutRecord(r record.Record, id transaction.Id) bool {
	ok := om.mutableMemtable.Put(memtable.NewRecordWithTransaction(r, id))
	if !ok {
		return false
	}
	return true
}
