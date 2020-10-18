package db

import (
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// random Put ,get ,delete get with transaction id
// make sure sstable level to 2 (100M)
func TestNewDB(t *testing.T) {
	// todo go on
	//db, rol, maxKeyId := randomPutGetDelete(t, transaction.NewTransactionIdGenerator(0))
}

// todo restore memtable from log file
func TestOpenDBFromExitDBFiles(t *testing.T) {
	//	1. insert random data to db
	//	2. close db
	//	3. open db with in the same dir
	//  4. do some operation and check
	//	5. close db
}

func TestDB_GetDBSnapshot(t *testing.T) {
	//	1. insert random data to db
	//	2. get transaction
	//	3. change original db
	//	4. get data from transaction, won't change
	//	5. test range query
}

func TestDB_concurrent(t *testing.T) {
	//	1. insert random data to db
	//	2. create multiple go routine to do random operation to db, test read with transaction id
}

const (
	PUT    = "PUT"
	DELETE = "DELETE"
)

type RecordOpLog struct {
	log map[record.Key][]RecordOp
}

func NewRecordOpLog() *RecordOpLog {
	return &RecordOpLog{make(map[record.Key][]RecordOp)}
}
func (rl *RecordOpLog) GetLastOP(key record.Key) RecordOp {
	res, ok := rl.log[key]
	if !ok {
		panic("not found")
	}
	return res[len(res)-1]
}

func (rl *RecordOpLog) Put(op RecordOp) {
	rl.log[op.data.Key()] = append(rl.log[op.data.Key()], op)
}

type RecordOp struct {
	data record.Record
	op   string
	id   transaction.Id
}

// return max id
func randomPutGetDelete(t *testing.T, idc chan transaction.Id) (*DB, *RecordOpLog, int) {
	dirName, err := ioutil.TempDir("", "test_DB")
	assert.Nil(t, err)

	db := OpenDB(dirName)

	rol := NewRecordOpLog()

	maxKeyId := 0
	// Put 100 record
	for i := 0; i < 100; i++ {
		r := toRecord(i)
		id := <-idc
		db.Put(r, id)
		rol.Put(RecordOp{r, PUT, id})
		maxKeyId = i
	}

	// 70% read 20% Put 10% delete
	for i := 0; i < 1000; i++ {
		randRes := rand.Intn(10)
		// read
		if randRes < 7 {
			id := rand.Intn(maxKeyId)
			r := toRecord(id)
			lastOP := rol.GetLastOP(r.Key())
			res, ok := db.Get(r.Key())
			if lastOP.op == PUT {
				assert.True(t, ok)
				assert.Equal(t, lastOP.data, res)
			}
			//	Put
		} else if randRes < 9 {
			id := maxKeyId + 1
			maxKeyId++
			r := toRecord(id)
			db.Put(r, <-idc)
			//	delete
		} else {
			id := rand.Intn(maxKeyId)
			r := toRecord(id)
			snapshotId := <-idc
			db.Delete(r.Key(), snapshotId)
			rol.Put(RecordOp{r, DELETE, snapshotId})
		}
	}
	return db, rol, maxKeyId
}

func toRecord(id int) record.Record {
	return record.NewRecordStr("key_"+strconv.Itoa(id), "value_"+strconv.Itoa(id)+"_"+
		strconv.Itoa(time.Now().Nanosecond()))
}
