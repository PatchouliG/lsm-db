package sstable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/storage/block"
	log "github.com/sirupsen/logrus"
	"os"
)

type Writer struct {
	file            *os.File
	blockFirstKey   []record.Key
	dataBlockNumber int
	bw              *block.Writer
}

func NewSStableWriter(fileName string) (*Writer, error) {
	res := Writer{}
	fa, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.WithField("err", err).
			Panicf("open file %s fail", fileName)
	}
	res.file = fa
	res.bw = block.NewWriter()

	return &res, nil
}
func (ssf *Writer) Write(r record.Record) bool {
	ok := ssf.bw.Write(r.Encode())
	if !ok {
		ssfWriteRes := ssf.write(ssf.bw.Byte())
		if !ssfWriteRes {
			log.Panic("ssf should has space")
		}
		if !ssf.HasSpace() {
			return false
		}
		ssf.bw = block.NewWriter()
		ok := ssf.bw.Write(r.Encode())
		if !ok {
			log.Panicf("sstable %s write only one record to block fail", r.String())
		}
	}
	return true
}

// return false if no more space
func (ssf *Writer) write(blockData []byte) bool {
	if ssf.dataBlockNumber == sstableDataBlockNumber {
		return false
	}

	ssf.dataBlockNumber += 1

	// add first record
	firstRecord, ok := record.FirstRecord(blockData)
	if !ok {
		log.Panic("get first record fail")
	}
	ssf.blockFirstKey = append(ssf.blockFirstKey, firstRecord.Key())

	// write to file
	_, err := ssf.file.Write(blockData)
	if err != nil {
		log.Panic("write sst file fail:", err)
	}
	return true
}

// has space to write record block
func (ssf *Writer) HasSpace() bool {
	return ssf.dataBlockNumber < sstableDataBlockNumber
}

func (ssf *Writer) FlushToFile() error {
	// write block to ssf
	data := ssf.bw.Byte()
	if len(data) > 0 {
		ssfWriteRes := ssf.write(data)
		if !ssfWriteRes {
			log.Panic("ssf should has space")
		}
	}

	if ssf.dataBlockNumber == 0 {
		log.Panic("empty sstable file should not be flushed")
	}

	// pad block
	if ssf.dataBlockNumber != sstableDataBlockNumber {
		padData := make([]byte, (sstableDataBlockNumber-ssf.dataBlockNumber)*block.DataBlockSizeInByte)
		_, err := ssf.file.Write(padData)
		if err != nil {
			log.Panic("write file err")
		}
	}

	// build record index
	bw := block.NewWriter()
	kw := record.NewKeyWriter()
	for _, key := range ssf.blockFirstKey {
		kw.Writer(key)
	}
	bw.Write(kw.Byte())
	dbData := bw.Byte()

	_, err := ssf.file.Write(dbData)
	if err != nil {
		return err
	}
	err = ssf.file.Sync()
	if err != nil {
		return err
	}
	err = ssf.file.Close()
	if err != nil {
		return err
	}
	return nil
}
