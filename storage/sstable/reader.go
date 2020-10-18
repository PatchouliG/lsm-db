package sstable

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/storage/block"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sort"
)

const (
	sstableMetaDataSizeLimit = 32 * 1024
	sstableFileSize          = 2 * 1024 * 1024
	sstableDataBlockNumber   = (sstableFileSize - sstableMetaDataSizeLimit) / block.DataBlockSizeInByte
	metaDataOffset           = sstableFileSize - block.DataBlockSizeInByte
)

// 2MB
// block 1 (32Kb)
// block 2
// ...
// block N
// meta record block (32Kb)

type Reader struct {
	id       Id
	file     *os.File
	firstKey []record.Key
	position int
	rr       *record.Reader
}

func NewReader(id Id) *Reader {
	res := Reader{}
	// empty reader
	res.rr = &record.Reader{}

	fileName := gloablConfig.SStableName(id.Id)
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		log.Panic("open sstable fail ", err)
	}
	res.file = f
	// build key index
	data := make([]byte, block.DataBlockSizeInByte)
	n, err := res.file.ReadAt(data, metaDataOffset)
	if err != nil {
		log.WithField("err", err).
			Panicf("read block from %s fail", res.file.Name())
	}

	if n != block.DataBlockSizeInByte {
		log.Panicf("read block size is %d, should be %d", n, block.DataBlockSizeInByte)
	}
	br := block.NewReader(data)
	ki := record.NewKeyReader(br.Byte())

	for ki.HasNext() {
		key := ki.Next()
		res.firstKey = append(res.firstKey, key)
	}
	return &res
}

func (r *Reader) Reset() {
	r.position = 0
	_, err := r.file.Seek(0, 0)
	if err != nil {
		log.WithField("err", err).
			Panicf("seek file %s fail", r.file.Name())
	}
}

// return ture if found
func (r *Reader) Find(key record.Key) (record.Record, bool) {
	br, ok := r.findBlockMayContain(key)
	if !ok {
		return record.Record{}, false
	}

	ri := record.NewRecordReader(br.Byte())
	recordFound, ok := ri.FindBy(key)
	return recordFound, ok

}

func (r *Reader) Next() (record.Record, bool) {
	res, ok := r.rr.Next()
	if !ok {
		br, ok := r.nextBlock()
		if !ok {
			return record.Record{}, false
		}
		r.rr = record.NewRecordReader(br.Byte())
		res, ok = r.rr.Next()
		if !ok {
			return record.Record{}, false
		}
	}

	return res, true
}

// todo add record block cache
// false if not find
func (r *Reader) findBlockMayContain(key record.Key) (block.Reader, bool) {
	res := searchKey(r.firstKey, key)
	if res == len(r.firstKey) || res == 0 {
		return block.Reader{}, false
	}

	data := make([]byte, block.DataBlockSizeInByte)
	_, err := r.file.ReadAt(data, int64(block.DataBlockSizeInByte*(res-1)))
	if err != nil {
		log.WithField("err", err).
			Panicf("read file %s fail", r.file.Name())
	}

	return block.NewReader(data), true
}

// return false if no data
func (r *Reader) nextBlock() (block.Reader, bool) {
	if r.position == len(r.firstKey) {
		return block.Reader{}, false
	}
	data := make([]byte, block.DataBlockSizeInByte)
	n, err := r.file.Read(data)

	r.position += 1

	if n == 0 && err == io.EOF {
		return block.Reader{}, false
	}
	if n != block.DataBlockSizeInByte {
		log.Panic("read data size is not ", block.DataBlockSizeInByte)
	}
	if err != nil {
		log.Panic("read block err", err)
	}
	return block.NewReader(data), true
}

func (r *Reader) fileName() string {
	return r.file.Name()
}

// return len(keys) if not found
func searchKey(keys []record.Key, k record.Key) int {
	return sort.Search(len(keys), func(i int) bool {
		return keys[i].Value() > k.Value()
	})
}
