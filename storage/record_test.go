package storage

import (
	"github.com/PatchouliG/wisckey-db/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecordEncodeAndDecode(t *testing.T) {
	key := common.NewKey("key")
	value := common.NewValue("value")
	r := NewRecord(key, value)
	data := r.encode()
	m, size := NewRecordFromByte(data)
	assert.Equal(t, m.getKey(), key)
	mValue, err := m.getValue()
	assert.Nil(t, err)
	assert.Equal(t, value, mValue, value)
	assert.Equal(t, len(data), int(size))
}
func TestRecordBasic(t *testing.T) {
	key := common.NewKey("key")
	value := common.NewValue("value")
	r := NewRecord(key, value)
	assert.False(t, r.isDeleted())

	assert.Equal(t, r.getKey(), key)
	mValue, err := r.getValue()
	assert.Nil(t, err)
	assert.Equal(t, value, mValue, value)

	dr := NewDeleteRecord(key)
	assert.True(t, dr.isDeleted())
}
func TestNewRecordIterator(t *testing.T) {
	r1 := NewRecord("1", "2")
	r2 := NewRecord("2", "3")
	r3 := NewRecord("123234", "123dssssssre2")
	var data []byte
	data = append(data, r1.encode()...)
	data = append(data, r2.encode()...)
	data = append(data, r3.encode()...)

	ri := NewRecordIterator(data)

	assert.Equal(t, ri.hasNext(), true)

	res, _ := ri.next()
	assert.Equal(t, r1, res)
	res, _ = ri.next()
	assert.Equal(t, r2, res)
	res, _ = ri.next()
	assert.Equal(t, r3, res)

	assert.False(t, ri.hasNext())

}
