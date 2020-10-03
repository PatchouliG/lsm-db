package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemtable(t *testing.T) {
	mt := NewMemtable()
	a := record.NewRecordStr("key1", "value1")
	b := record.NewRecordStr("key2", "value2")
	c := record.NewRecordStr("key2", "value3")
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
