package lsm

import (
	"github.com/PatchouliG/lsm-db/memtable"
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
	"log"
)

type twoMemtable struct {
	id Id
	// level -> sstable metadata
	levelInfo *levelInfo
	// handle write operation (put delete)
	mutableMemtable   *memtable.Memtable
	immutableMemtable *memtable.Memtable
}

// return a oneMemtable, contains all info in old oneMemtable an old memtable, and a new memtable
func (om *oneMemtable) newTwoMemtable() *twoMemtable {
	// todo
	panic("")
}

// create new lsm use new levelInfo
func (tm *twoMemtable) oneMemtable(info *levelInfo) *oneMemtable {
	res := &oneMemtable{NextId(), info, tm.mutableMemtable}
	err := tm.immutableMemtable.Discard()
	if err != nil {
		log.Panic(err)
	}
	return res
}

// false if not found or deleted
func (tm *twoMemtable) Get(key record.Key) (record.Record, bool) {
	if res, ok := tm.mutableMemtable.Get(key); ok {
		return res.Record, ok
	}
	if res, ok := tm.immutableMemtable.Get(key); ok {
		return res.Record, ok
	}
	return tm.levelInfo.get(key)
}

func (tm *twoMemtable) PutRecord(r record.Record, id transaction.Id) bool {
	ok := tm.mutableMemtable.Put(memtable.NewRecordWithTransaction(r, id))
	if !ok {
		return false
	}
	return true
}
