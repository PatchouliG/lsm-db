package lsm

import (
	"github.com/PatchouliG/wisckey-db/memtable"
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/transaction"
)

// when sstable write fail (log file size limit)
// 1. create new lsm with two memtable,mutable(new) and immutable(old)
// 2. use the new lsm as latest lsm
// 3. write the new record to new mutable memtable
// 4. create write routine to save the old memtable to sstable
// 5. when write finish,create new lsm (only new memtable and new sstables),update latest lsm
// 6. discard old memtable, old sstable (if no transaction)

type levelInfo map[int][]sstableMetaData

type Lsm struct {
	id Id
	// level -> sstable metadata
	levelInfo levelInfo

	// handle write opertion(put delete)
	mutableMemtable *memtable.Memtable
	// not nil if the background write memtable to sstable is in progress
	immutableMemtable *memtable.Memtable
}

type Snapshot struct {
	// read only
	*Lsm
	id transaction.Id
}

func NewLsm() *Lsm {
	// todo
	panic("")
}

func (l *Lsm) NewSnapshot() *Snapshot {
	panic("")
}

// ture if exits
// todo two memtable may exits
func (l *Lsm) Get(key record.Key) (record.Record, bool) {
	//todo
	panic("")
}

// work for put and delete
// false if memtable write fail(log file size reaches limit)

// own by routine and metadata
func (l *Lsm) AddRecord(r record.Record) bool {
	panic("")
}

// return a lsm, contains all info in old lsm an old memtable, and a new memtable
func (l *Lsm) newLsmWithEmptyMemtable() *Lsm {
	panic("")
}

// use old lsm memtable and level info pass
func (l *Lsm) newLsmWithLeveInfo(info levelInfo) *Lsm {
	panic("")
}

// write memtable to level 0,may cause other compaction to produce new sstable
// return sstable metadata in every level
func (l *Lsm) compact() levelInfo {
	panic("")
}

//type DB interface {
//	return until flush to disk
//Put(key string, value string) error
//Get(Key string) (value string, exit bool, err error)
//Delete(Key string) error
//Close() error
//}
//
