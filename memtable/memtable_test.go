package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/PatchouliG/wisckey-db/transaction"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemtable(t *testing.T) {
	mt := NewMemtable()
	a := NewRecordWithTransaction(record.NewRecordStr("key1", "value1"), transaction.MockId(1))
	b := NewRecordWithTransaction(record.NewRecordStr("key2", "value2"), transaction.MockId(2))
	c := NewRecordWithTransaction(record.NewRecordStr("key2", "value2"), transaction.MockId(3))
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
	ok = mt.Delete(record.NewKey("not exits"))
	assert.False(t, ok)

	// test delete success
	ok = mt.Delete(a.Key())
	assert.True(t, ok)

	_, ok = mt.Get(a.Key())
	assert.False(t, ok)

}
func TestClone(t *testing.T) {
	mt := NewMemtable()
	a := NewRecordWithTransaction(record.NewRecordStr("key1", "value1"), transaction.MockId(1))
	b := NewRecordWithTransaction(record.NewRecordStr("key2", "value2"), transaction.MockId(2))
	mt.Put(a)
	mt.Put(b)

	mtCloned := mt.Clone()

	res, ok := mtCloned.Get(a.Key())
	assert.True(t, ok)
	assert.Equal(t, a, res)

	res, ok = mtCloned.Get(b.Key())
	assert.True(t, ok)
	assert.Equal(t, b, res)

	_, ok = mtCloned.Get(record.NewKey("not exits"))
	assert.False(t, ok)

	// delete cloned memtable won't change original memtable
	ok = mtCloned.Delete(a.Key())
	assert.True(t, ok)

	_, ok = mtCloned.Get(a.Key())
	assert.False(t, ok)

	res, ok = mt.Get(a.Key())
	assert.True(t, ok)
	assert.Equal(t, a, res)

}
