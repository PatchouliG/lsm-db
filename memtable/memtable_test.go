package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"strconv"
	"testing"
)

var sg chan snapshot.Id

func init() {
	sg = make(chan snapshot.Id)
	snapshot.SetStartId(0)
	go func() {
		for {
			sg <- snapshot.NextId()
		}
	}()

	dir, err := ioutil.TempDir("", "memtable")
	if err != nil {
		panic("create tmp dir err")
	}
	setConfig(Config{0, dir})
}

func TestMemtableBasic(t *testing.T) {
	mt := NewMemtable()
	a := NewRecordWithTransaction(record.NewRecordStr("key1", "value1"), <-sg)
	b := NewRecordWithTransaction(record.NewRecordStr("key2", "value2"), <-sg)
	c := NewRecordWithTransaction(record.NewRecordStr("key2", "value2"), <-sg)
	mt.Put(a)
	mt.Put(b)

	// test get
	res, ok := mt.Get(a.Key())
	assert.True(t, ok)
	assert.Equal(t, a, res)

	// test overwrite
	mt.Put(c)
	res, ok = mt.Get(c.Key())
	assert.True(t, ok)
	assert.Equal(t, c, res)

	// test not get found
	_, ok = mt.Get(record.NewKey("not exits"))
	assert.False(t, ok)

	// test delete get found
	ok = mt.Delete(record.NewKey("not exits"), <-sg)
	assert.False(t, ok)

	// test delete success
	ok = mt.Delete(a.Key(), <-sg)
	assert.True(t, ok)

	_, ok = mt.Get(a.Key())
	assert.False(t, ok)

}

func TestSnapshotOperation(t *testing.T) {
	mt := NewMemtable()
	oldId := <-sg
	a := NewRecordWithTransaction(record.NewRecordStr("key1", "value1"), <-sg)
	b := NewRecordWithTransaction(record.NewRecordStr("key1", "value2"), <-sg)

	mt.Put(a)
	mt.Put(b)

	res, ok := mt.GetWithSnapshot(b.Key(), b.Id)
	assert.True(t, ok)
	assert.Equal(t, b, res)

	res, ok = mt.GetWithSnapshot(a.Key(), a.Id)
	assert.True(t, ok)
	assert.Equal(t, a, res)

	res, ok = mt.GetWithSnapshot(b.Key(), <-sg)
	assert.True(t, ok)
	assert.Equal(t, b, res)

	// test old snapshot id
	_, ok = mt.GetWithSnapshot(b.Key(), oldId)
	assert.False(t, ok)

	// test not exits
	_, ok = mt.GetWithSnapshot(record.NewKey("not exits"), <-sg)
	assert.False(t, ok)

	//	test delete and get snapshot
	mt.Delete(b.Key(), <-sg)
	res, ok = mt.GetWithSnapshot(b.Key(), b.Id)
	assert.True(t, ok)
	assert.Equal(t, b, res)

	_, ok = mt.GetWithSnapshot(b.Key(), <-sg)
	assert.False(t, ok)

}

func TestWriteMemtableUtilFull(t *testing.T) {
	mt := NewMemtable()
	rc := randomRecordChan()
	var recordStore []RecordWithTransaction
	for {
		r := <-rc
		sid := <-sg
		res := mt.Put(NewRecordWithTransaction(r, sid))
		if !res {
			break
		}
		recordStore = append(recordStore, RecordWithTransaction{r, sid})
		// check
		if rand.Intn(10) == 0 {
			r := recordStore[rand.Intn(len(recordStore)-1)]
			res, ok := mt.GetWithSnapshot(r.Key(), r.Id)
			assert.True(t, ok)
			assert.Equal(t, r, res)
		}
	}
}

// unique key
func randomRecordChan() chan record.Record {
	res := make(chan record.Record)
	go func() {
		for {
			pool := []byte("23423wfsddafw34t24tfwaserf809q234yfhq93ghq09ty223423423dasdweyf4p98f4y")
			count := 0
			key := string(pool[rand.Intn(len(pool))]) + "_" + strconv.Itoa(count)
			count++
			value := string(pool[rand.Intn(len(pool))]) + strconv.Itoa(count)
			count++
			res <- record.NewRecordStr(key, value)
		}
	}()
	return res
}
