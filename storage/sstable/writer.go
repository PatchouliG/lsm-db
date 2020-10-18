package sstable

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/storage/block"
	log "github.com/sirupsen/logrus"
	"os"
)

type Writer struct {
	id              Id
	file            *os.File
	blockFirstKey   []record.Key
	dataBlockNumber int
	bw              *block.Writer
}

func NewWriter() (*Writer, error) {
	res := Writer{}
	id := NextId()
	fileName := gloablConfig.SStableName(id.Id)
	fa, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.WithField("err", err).
			Panicf("open file %s fail", fileName)
	}
	res.id = id
	res.file = fa
	res.bw = block.NewWriter()

	return &res, nil
}
func (ssw *Writer) Write(r record.Record) bool {
	ok := ssw.bw.Write(r.Encode())
	if !ok {
		ssfWriteRes := ssw.write(ssw.bw.Byte())
		if !ssfWriteRes {
			log.Panic("ssw should has space")
		}
		if !ssw.HasSpace() {
			return false
		}
		ssw.bw = block.NewWriter()
		ok := ssw.bw.Write(r.Encode())
		if !ok {
			log.Panicf("sstable %s write only one record to block fail", r.String())
		}
	}
	return true
}

// return false if no more space
func (ssw *Writer) write(blockData []byte) bool {
	if ssw.dataBlockNumber == sstableDataBlockNumber {
		return false
	}

	ssw.dataBlockNumber += 1

	// add first record
	firstRecord, ok := record.FirstRecord(blockData)
	if !ok {
		log.Panic("get first record fail")
	}
	ssw.blockFirstKey = append(ssw.blockFirstKey, firstRecord.Key())

	// write to file
	_, err := ssw.file.Write(blockData)
	if err != nil {
		log.Panic("write sst file fail:", err)
	}
	return true
}

func (ssw *Writer) FlushToFile() error {
	// write block to ssw
	data := ssw.bw.Byte()
	if len(data) > 0 {
		ssfWriteRes := ssw.write(data)
		if !ssfWriteRes {
			log.Panic("ssw should has space")
		}
	}

	if ssw.dataBlockNumber == 0 {
		log.Panic("empty sstable file should not be flushed")
	}

	// pad block
	if ssw.dataBlockNumber != sstableDataBlockNumber {
		padData := make([]byte, (sstableDataBlockNumber-ssw.dataBlockNumber)*block.DataBlockSizeInByte)
		_, err := ssw.file.Write(padData)
		if err != nil {
			log.Panic("write file err")
		}
	}

	// build record index
	bw := block.NewWriter()
	kw := record.NewKeyWriter()
	for _, key := range ssw.blockFirstKey {
		kw.Writer(key)
	}
	bw.Write(kw.Byte())
	dbData := bw.Byte()

	_, err := ssw.file.Write(dbData)
	if err != nil {
		return err
	}
	err = ssw.file.Sync()
	if err != nil {
		return err
	}
	err = ssw.file.Close()
	if err != nil {
		return err
	}
	return nil
}

// has space to write record block
func (ssw *Writer) HasSpace() bool {
	return ssw.dataBlockNumber < sstableDataBlockNumber
}

func (ssw *Writer) Id() Id {
	return ssw.id
}
