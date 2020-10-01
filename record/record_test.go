package record

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecordEncodeAndDecode(t *testing.T) {
	key := NewKey("record")
	value := NewValue("value")
	r := NewRecord(key, value)
	data := r.Encode()
	m, size := NewRecordFromByte(data)
	assert.Equal(t, m.Key(), key)
	mValue, err := m.Value()
	assert.Nil(t, err)
	assert.Equal(t, value, mValue, value)
	assert.Equal(t, len(data), int(size))
}
func TestRecordBasic(t *testing.T) {
	key := NewKey("record")
	value := NewValue("value")
	r := NewRecord(key, value)
	assert.False(t, r.IsDeleted())

	assert.Equal(t, r.Key(), key)
	mValue, err := r.Value()
	assert.Nil(t, err)
	assert.Equal(t, value, mValue, value)

	dr := NewDeleteRecord(key)
	assert.True(t, dr.IsDeleted())
}
func TestNewRecordIterator(t *testing.T) {
	r1 := NewRecord(NewKey("1"), NewValue("2"))
	r2 := NewRecord(NewKey("2"), NewValue("3"))
	r3 := NewRecord(NewKey("123234"), NewValue("123dssssssre2"))

	//dr := NewDeleteRecord(NewKey("delete"))

	rw := NewWriter()
	rw.Write(r1)
	rw.Write(r2)
	rw.Write(r3)

	data := rw.Byte()

	ri := NewRecordReader(data)

	rf, ok := FirstRecord(data)
	assert.True(t, ok)
	assert.Equal(t, r1, rf)

	assert.Equal(t, ri.HasNext(), true)

	res, _ := ri.Next()
	assert.Equal(t, r1, res)
	res, _ = ri.Next()
	assert.Equal(t, r2, res)
	res, _ = ri.Next()
	assert.Equal(t, r3, res)

	assert.False(t, ri.HasNext())

	res, ok = ri.FindBy(r2.Key())
	assert.True(t, ok)
	assert.Equal(t, r2, res)

	_, ok = ri.FindBy(NewKey("not found"))
	assert.False(t, ok)

	// check position reset
	assert.False(t, ri.HasNext())

}

func TestStringer(t *testing.T) {
	a := NewRecord(NewKey("key"), NewValue("value"))
	assert.Equal(t, "{key:{key}, isDelete: false,value: {value}}", a.String())
}
