package memtable

import (
	"github.com/PatchouliG/wisckey-db/gloablConfig"
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
	"github.com/PatchouliG/wisckey-db/storage/sstable"
	log "github.com/sirupsen/logrus"
	"os"
	"sort"
	"sync"
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
	id Id
	//m    [record.Key][]RecordWithTransaction
	// key -> RecordWithTransaction
	m   sync.Map
	lfw *logFileWriter
}

func NewMemtable() *Memtable {
	i := NextId()
	return &Memtable{id: i, m: sync.Map{},
		lfw: NewLogFileWriter(gloablConfig.LogFileName(i.id))}
}

//	todo
func restoreFromLogFile() *Memtable {
	panic("")
}

// default async to write log file
// return false if log file size reach limit
func (mt *Memtable) Put(rt RecordWithTransaction) bool {
	res := mt.lfw.Write(rt.Record)
	if !res {
		return false
	}
	mt.putToMemtable(rt)
	return true
}

func (mt *Memtable) putToMemtable(rt RecordWithTransaction) {
	if rs, ok := mt.m.Load(rt.Key()); ok {
		rs = append(rs.([]RecordWithTransaction), rt)
		mt.m.Store(rt.Key(), rs)
	} else {
		mt.m.Store(rt.Key(), []RecordWithTransaction{rt})
	}
}

// false if not found
// return latest record
func (mt *Memtable) Get(key record.Key) (RecordWithTransaction, bool) {
	rs, ok := mt.m.Load(key)
	if !ok {
		return RecordWithTransaction{}, false
	}
	rst := rs.([]RecordWithTransaction)
	res := rst[len(rst)-1]
	if res.IsDeleted() {
		return RecordWithTransaction{}, false
	}
	return res, true
}

func (mt *Memtable) GetWithSnapshot(key record.Key, id snapshot.Id) (res RecordWithTransaction, found bool) {
	rst, ok := mt.m.Load(key)
	if !ok {
		found = false
		return
	}
	rs := rst.([]RecordWithTransaction)
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
	rst, ok := mt.m.Load(key)
	if !ok {
		return false
	}
	rs := rst.([]RecordWithTransaction)
	mt.m.Store(key, append(rs, NewRecordWithTransaction(record.NewDeleteRecord(key), id)))
	return true
}

// delete log file if memtable is unusable (discard)
func (mt *Memtable) Discard() error {
	logFileName := gloablConfig.LogFileName(mt.id.id)
	log.WithField("log file", logFileName).Info("memtable discard , delete log file")
	err := os.Remove(logFileName)
	if err != nil {
		log.WithError(err).
			WithField("log file", logFileName).
			Error("remove log file fail")
		return err
	}
	return nil
}

type recordIteratorImp struct {
	recordSlice []record.Record
	position    int
}

func (r *recordIteratorImp) Next() (record.Record, bool) {
	if r.position < len(r.recordSlice) {
		res := r.recordSlice[r.position]
		r.position++
		return res, true
	}
	return record.Record{}, false
}

func (mt *Memtable) toRecordIterator() record.Iterator {
	var recordSlice []record.Record
	mt.m.Range(func(key, value interface{}) bool {
		typedValue := value.([]RecordWithTransaction)
		lastRecord := typedValue[len(typedValue)-1]
		recordSlice = append(recordSlice, lastRecord.Record)
		return true
	})
	sort.Slice(recordSlice, func(i, j int) bool {
		return recordSlice[i].Key().Value() < recordSlice[j].Key().Value()
	})
	return &recordIteratorImp{recordSlice, 0}
}

func (mt *Memtable) ToSStable() []*sstable.ReaderWithKeyRange {
	ri := mt.toRecordIterator()
	res := sstable.BuildSStable([]record.Iterator{ri})
	return res
}
