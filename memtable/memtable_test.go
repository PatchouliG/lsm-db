package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/snapshot"
	"github.com/stretchr/testify/assert"
	"testing"
)

var sg chan snapshot.Id

func init() {
	sg = make(chan snapshot.Id)
	id := 0
	go func() {
		for {
			sg <- snapshot.MockId(int64(id))
			id++
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
