package lsm

import (
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
	"log"
	"sync"
)

const lsmOneMemtable = "lsmOneMemtable"

// when flush memtable is in process
const lsmTwoMemtable = "lsmTwoMemtable"

// thread safe struct
type Lsm struct {
	*oneMemtable
	*twoMemtable
	status string
	lock   sync.Mutex
}

func NewLsm() *Lsm {
	return &Lsm{NewOneMemtable(), nil, lsmOneMemtable, sync.Mutex{}}
}

// false if is deleted
func (l *Lsm) Get(key record.Key) (record.Record, bool) {
	l.lock.Lock()
	defer l.lock.Unlock()

	switch l.status {
	case lsmOneMemtable:
		return l.oneMemtable.Get(key)
	case lsmTwoMemtable:
		return l.twoMemtable.Get(key)
	}
	log.Panic("lsm status match fail")
	return record.Record{}, false
}

// return false if memtable is full and flush is in progress
func (l *Lsm) Put(r record.Record, id transaction.Id) bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	switch l.status {
	case lsmOneMemtable:
		if ok := l.oneMemtable.PutRecord(r, id); ok {
			return true
		}
		lfc, tw := l.oneMemtable.newLsmForCompaction()
		l.status = lsmTwoMemtable
		l.oneMemtable = nil
		l.twoMemtable = tw
		go func(lfc *lsmForCompaction) {
			li := lfc.FlushMemtable()
			// update to one memtable
			l.flushMemtableFinish(li)
			//return l.oneMemtable.PutRecord(r,id)
		}(lfc)
		return l.twoMemtable.PutRecord(r, id)

	case lsmTwoMemtable:
		return l.twoMemtable.PutRecord(r, id)
	}
	log.Panic("lsm status match fail")
	return false
}
func (l *Lsm) levelHeight() int {
	switch l.status {
	case lsmOneMemtable:
		return l.oneMemtable.levelInfo.height()
	case lsmTwoMemtable:
		return l.twoMemtable.levelInfo.height()
	}
	log.Panic("lsm status match fail")
	return 0
}

func (l *Lsm) flushMemtableFinish(info *levelInfo) {
	l.lock.Lock()
	defer l.lock.Unlock()

	om := l.twoMemtable.oneMemtable(info)
	l.status = lsmOneMemtable
	l.twoMemtable = nil
	l.oneMemtable = om
}

// todo
func (l *Lsm) ToSnapshot(key record.Key) *Snapshot {
	panic("")
}
