package sstable

import (
	"container/heap"
	"github.com/PatchouliG/wisckey-db/record"
	"log"
)

func Compaction(rs []*Reader, outputFileChan chan string) []*Reader {
	var ri []record.Iterator
	for _, reader := range rs {
		ri = append(ri, reader)
	}
	return compaction(ri, outputFileChan)
}

// return output sstable file reader
func compaction(ris []record.Iterator, outputFileChan chan string) []*Reader {
	generator := newRecordGenerator(ris)

	sstw := getSStw(outputFileChan)
	var res []*Reader
	for {
		r, ok := generator.next()
		// no more record, Flush sstw, return
		if !ok {
			flush(sstw)
			res = append(res, NewReader(sstw.file.Name()))
			break
		}

		// write record to sstw
		ok = sstw.Write(r)
		// write fail, Flush sstw, create new sstw
		if !ok {
			flush(sstw)
			res = append(res, NewReader(sstw.file.Name()))
			sstw = getSStw(outputFileChan)
			ok = sstw.Write(r)
			if !ok {
				log.Panic("write empty sstable file fail")
			}
		}
	}
	return res
}

func flush(sstw *Writer) {
	err := sstw.FlushToFile()
	if err != nil {
		log.Panic("Flush sstable fail ", err)
	}
}

func getSStw(outputFileChan chan string) *Writer {
	f := <-outputFileChan
	sstw, err := NewSStableWriter(f)
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
