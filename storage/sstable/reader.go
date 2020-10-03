package sstable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/storage/block"
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
	file     *os.File
	firstKey []record.Key
	position int
	rr       *record.Reader
}

func NewReader(file string) *Reader {
	res := Reader{}
	// empty reader
	res.rr = &record.Reader{}

	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
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

func (ssf *Reader) Reset() {
	ssf.position = 0
	_, err := ssf.file.Seek(0, 0)
	if err != nil {
		log.WithField("err", err).
			Panicf("seek file %s fail", ssf.file.Name())
	}
}

// return ture if found
func (ssf *Reader) Find(key record.Key) (record.Record, bool) {
	br, ok := ssf.findBlockMayContain(key)
	if !ok {
		return record.Record{}, false
	}

	ri := record.NewRecordReader(br.Byte())
	r, ok := ri.FindBy(key)
	return r, ok

}

func (ssf *Reader) Next() (record.Record, bool) {
	res, ok := ssf.rr.Next()
	if !ok {
		br, ok := ssf.nextBlock()
		if !ok {
			return record.Record{}, false
		}
		ssf.rr = record.NewRecordReader(br.Byte())
		res, ok = ssf.rr.Next()
		if !ok {
			return record.Record{}, false
		}
	}

	return res, true
}

// todo add record block cache
// false if not find
func (ssf *Reader) findBlockMayContain(key record.Key) (block.Reader, bool) {
	res := searchKey(ssf.firstKey, key)
	if res == len(ssf.firstKey) || res == 0 {
		return block.Reader{}, false
	}

	data := make([]byte, block.DataBlockSizeInByte)
	_, err := ssf.file.ReadAt(data, int64(block.DataBlockSizeInByte*(res-1)))
	if err != nil {
		log.WithField("err", err).
			Panicf("read file %s fail", ssf.file.Name())
	}

	return block.NewReader(data), true
}

// return false if no data
func (ssf *Reader) nextBlock() (block.Reader, bool) {
	if ssf.position == len(ssf.firstKey) {
		return block.Reader{}, false
	}
	data := make([]byte, block.DataBlockSizeInByte)
	n, err := ssf.file.Read(data)

	ssf.position += 1

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

// return len(keys) if not found
func searchKey(keys []record.Key, k record.Key) int {
	return sort.Search(len(keys), func(i int) bool {
		return keys[i].Value() > k.Value()
	})
}
