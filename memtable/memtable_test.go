package memtable

import (
	"github.com/PatchouliG/wisckey-db/gloablConfig"
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
)

var sg chan snapshot.Id

func init() {
	gloablConfig.UseTestConfig()
	sg = make(chan snapshot.Id)
	go func() {
		for {
			sg <- snapshot.NextId()
		}
	}()
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

	err := mt.Discard()
	assert.Nil(t, err)

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

	err := mt.Discard()
	assert.Nil(t, err)

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
	err := mt.Discard()
	assert.Nil(t, err)

}

// unique key
func randomRecordChan() chan record.Record {
	res := make(chan record.Record)
	count := 0
	pool := []byte("23423w34t24ta809q234yf93ghq09ty223423423daf4p98f4y")
	go func() {
		for {
			key := "key_" + string(pool[rand.Intn(len(pool))]) + "_" + strconv.Itoa(count)
			count++
			value := "value_" + string(pool[rand.Intn(len(pool))]) + strconv.Itoa(count)
			count++
			res <- record.NewRecordStr(key, value)
		}
	}()
	return res
}

func TestToSstable(t *testing.T) {

	mt := NewMemtable()
	rc := randomRecordChan()

	// insert until full
	for {
		r := <-rc
		sid := <-sg
		ok := mt.Put(NewRecordWithTransaction(r, sid))
		if !ok {
			break
		}
	}

	ssts := mt.ToSStable()
	// should produce multiple sstable
	assert.Greater(t, len(ssts), 1)
	var lastKey record.Key
	//	check order by key
	for _, sstr := range ssts {
		for {
			r, ok := sstr.Next()
			if !ok {
				break
			}
			if len(lastKey.Value()) != 0 {
				assert.True(t, r.Key().Value() >= lastKey.Value())
			}
			lastKey = r.Key()
		}
	}
}
