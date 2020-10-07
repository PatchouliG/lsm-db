package memtable

import (
	"github.com/PatchouliG/wisckey-db/gloablConfig"
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
	"github.com/PatchouliG/wisckey-db/storage/sstable"
	log "github.com/sirupsen/logrus"
	"os"
	"sort"
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
	id  Id
	m   map[record.Key][]RecordWithTransaction
	lfw *logFileWriter
}

func NewMemtable() *Memtable {
	i := NextId()
	return &Memtable{id: i, m: make(map[record.Key][]RecordWithTransaction),
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
	for _, value := range mt.m {
		lastRecord := value[len(value)-1]
		recordSlice = append(recordSlice, lastRecord.Record)
	}
	// sort by key
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
