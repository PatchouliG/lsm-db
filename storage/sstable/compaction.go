package sstable

import (
	"container/heap"
	"github.com/PatchouliG/lsm-db/record"
	"log"
)

type ReaderWithKeyRange struct {
	*Reader
	StartKey record.Key
	EndKey   record.Key
}

func BuildSStableFromReader(rs []*Reader) []*ReaderWithKeyRange {
	var ri []record.Iterator
	for _, reader := range rs {
		ri = append(ri, reader)
	}
	return BuildSStable(ri)
}

// return output sstable file reader
func BuildSStable(ris []record.Iterator) []*ReaderWithKeyRange {
	generator := newRecordGenerator(ris)

	sstw := getSStw()
	var res []*ReaderWithKeyRange
	var startKey record.Key
	var lastWriteKey record.Key
	for {
		r, ok := generator.next()
		// no more record, Flush sstw, return
		if !ok {
			flush(sstw)
			res = append(res, NewReaderWithKeyRange(sstw, startKey, lastWriteKey))
			break
		}

		//  update start key if necessary
		if len(startKey.Value()) == 0 {
			startKey = r.Key()
		}

		// write record to sstw
		ok = sstw.Write(r)
		// write fail, Flush sstw, create new sstw
		if !ok {
			flush(sstw)
			res = append(res, NewReaderWithKeyRange(sstw, startKey, lastWriteKey))
			sstw = getSStw()
			ok = sstw.Write(r)
			if !ok {
				log.Panic("write empty sstable file fail")
			}
			// write to a new sstable success, update start key to empty
			startKey = record.Key{}
		}
		// write success update last write key
		lastWriteKey = r.Key()
	}
	return res
}

func NewReaderWithKeyRange(sstw *Writer, startKey record.Key, endKey record.Key) *ReaderWithKeyRange {
	if len(startKey.Value()) == 0 || len(endKey.Value()) == 0 {
		log.Panicf("start key %s or end key %s should not be empty", startKey.Value(), endKey.Value())
	}
	return &ReaderWithKeyRange{NewReader(sstw.Id()),
		startKey, endKey}
}

func flush(sstw *Writer) {
	err := sstw.FlushToFile()
	if err != nil {
		log.Panic("Flush sstable fail ", err)
	}
}

func getSStw() *Writer {
	sstw, err := NewSStableWriter()
	if err != nil {
		log.Panic("create new sstable file error ", err)
	}
	return sstw
}

// output next min key record
type recordGenerator struct {
	readers []record.Iterator
	rh      recordHeap
}

func newRecordGenerator(readers []record.Iterator) recordGenerator {
	res := recordGenerator{}
	for _, reader := range readers {
		r, ok := reader.Next()
		if !ok {
			continue
		}
		res.readers = append(res.readers, reader)
		heap.Push(&res.rh, recordWithReader{r, reader})
	}
	return res
}

func (g *recordGenerator) next() (record.Record, bool) {
	if g.rh.Len() == 0 {
		return record.Record{}, false
	}
	r := (heap.Pop(&g.rh)).(recordWithReader)

	res, reader := r.Record, r.Iterator
	nextRecord, ok := reader.Next()

	// delete reader from recordGenerator if no more record
	if !ok {
		for i, r := range g.readers {
			if r == reader {
				g.readers = append(g.readers[:i], g.readers[i+1:]...)
				break
			}
		}
	} else {
		heap.Push(&g.rh, recordWithReader{nextRecord, reader})
	}
	return res, true
}

type recordWithReader struct {
	record.Record
	record.Iterator
}

type recordHeap struct {
	rc []recordWithReader
}

func (r *recordHeap) Len() int {
	return len(r.rc)
}

func (r *recordHeap) Less(i, j int) bool {
	return r.rc[i].Record.Key().Value() < r.rc[j].Record.Key().Value()
}

func (r *recordHeap) Swap(i, j int) {
	tmp := r.rc[i]
	r.rc[i] = r.rc[j]
	r.rc[j] = tmp
}

func (r *recordHeap) Push(x interface{}) {
	r.rc = append(r.rc, x.(recordWithReader))
}

func (r *recordHeap) Pop() interface{} {
	old := r.rc
	n := len(old)
	x := old[n-1]
	r.rc = old[0 : n-1]
	return x
}
