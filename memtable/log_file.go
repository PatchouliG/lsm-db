package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/storage/block"
	log "github.com/sirupsen/logrus"
	"os"
)

// 4MB
const logFileSizeLimit = 4 * 1024 * 1024

// log file for memtable
// block *N
type logFileWriter struct {
	rw *record.Writer
	f  *os.File
	// current block number write to file
	blockNumberWritten int
}

func NewLogFileWriter(fileName string) *logFileWriter {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.WithField("err", err).
			Panic("create fileName for log fileName fail")
	}
	return &logFileWriter{record.NewWriter(), f, 0}
}

// false if no space
func (l *logFileWriter) Write(r record.Record) bool {
	data := r.Encode()
	if l.rw.Len()+len(data) >= block.MaxBlockSize() {
		l.writeOneBlockDataToFile()

		if l.blockNumberWritten == logFileSizeLimit/block.DataBlockSizeInByte {
			err := l.f.Sync()
			if err != nil {
				log.WithField("err", err).Panic("file sync fail")
			}
			err = l.f.Close()
			if err != nil {
				log.WithField("err", err).Panic("close file fail")
			}
			return false
		}

	}
	l.rw.Write(r)
	return true
}

// write current record writer data to file
func (l *logFileWriter) writeOneBlockDataToFile() {
	bw := block.NewWriter()
	ok := bw.Write(l.rw.Byte())
	if !ok {
		log.Panic("write to block fail")
	}

	_, err := l.f.Write(bw.Byte())
	if err != nil {
		log.WithField("err", err).
			Panic("write to file fail")
	}

	err = l.f.Sync()
	if err != nil {
		log.WithField("err", err).
			Panic("sync file fail")
	}

	l.blockNumberWritten++
	l.rw = record.NewWriter()
}

func (l *logFileWriter) Flush() {
	l.writeOneBlockDataToFile()
}

type logFileReader struct {
	f              *os.File
	rr             *record.Reader
	blockReadCount int
}

func newLogFileReader(file string) *logFileReader {
	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		log.WithField("err", err).
			WithField("file", file).
			Panic("open file error")
	}
	return &logFileReader{f, &record.Reader{}, 0}
}

// todo use for restore memtable after crash
func (l *logFileReader) Next() (record.Record, bool) {
	res, ok := l.rr.Next()
	if !ok {

		if l.blockReadCount == logFileSizeLimit/block.DataBlockSizeInByte {
			return record.Record{}, false
		}

		l.blockReadCount++
		data := make([]byte, block.DataBlockSizeInByte)
		n, err := l.f.Read(data)
		// no more block
		if err != nil {
			return record.Record{}, false
		}
		if n != block.DataBlockSizeInByte {
			log.Panicf("read block size is %d", n)
		}
		l.rr = record.NewRecordReader(data)

		res, ok = l.rr.Next()
		if !ok {
			log.Panic("read record should be ok")
		}
	}
	return res, true

}
