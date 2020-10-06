package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
	"path"
)

var logFileOutPutDir string

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
	return &Memtable{id: i, m: make(map[record.Key][]RecordWithTransaction), lfw: NewLogFileWriter(logFileName(i))}
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

func logFileName(id Id) string {
	return path.Join(logFileOutPutDir, "memtable_"+id.id.String()+"_logFile")
}
