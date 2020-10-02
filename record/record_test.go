package record

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestRecordEncodeAndDecode(t *testing.T) {
	key := NewKey("record")
	value := NewValue("value")
	r := NewRecordStr("record", "value")
	data := r.Encode()
	m, size, ok := NewRecordFromByte(data)
	assert.True(t, ok)
	assert.Equal(t, m.Key(), key)
	mValue, ok := m.Value()
	assert.True(t, ok)
	assert.Equal(t, value, mValue, value)
	assert.Equal(t, len(data), size)
}
func TestRecordBasic(t *testing.T) {
	key := NewKey("record")
	value := NewValue("value")
	r := NewRecord(key, value)
	assert.False(t, r.IsDeleted())

	assert.Equal(t, r.Key(), key)
	mValue, ok := r.Value()
	assert.True(t, ok)
	assert.Equal(t, value, mValue, value)

	dr := NewDeleteRecord(key)
	assert.True(t, dr.IsDeleted())
}
func TestNewRecordIterator(t *testing.T) {
	r1 := NewRecord(NewKey("1"), NewValue("2"))
	r2 := NewRecord(NewKey("2"), NewValue("3"))
	r3 := NewRecord(NewKey("123234"), NewValue("1232"))

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

func TestEmptyReader(t *testing.T) {
	a := Reader{}
	assert.False(t, a.HasNext())
}

func TestWriter_Len(t *testing.T) {
	w := NewWriter()
	a := NewRecord(NewKey("key"), NewValue("value"))
	w.Write(a)
	assert.Equal(t, len(a.Encode()), w.Len())
}
func TestReadWithCorruptData(t *testing.T) {
	w := NewWriter()
	a := NewRecord(NewKey("key"), NewValue("value"))
	w.Write(a)
	data := w.Byte()
	appendRandomDataAndCheck(t, data, a)

}

func appendRandomDataAndCheck(t *testing.T, data []byte, a Record) {
	for i := 0; i < 100; i++ {
		// append corrupt data
		dataAppended := append(data, randomByteInRandomLength(100)...)
		r := NewRecordReader(dataAppended)
		i, ok := r.Next()
		assert.True(t, ok)
		assert.Equal(t, a, i)

		_, ok = r.Next()
		assert.False(t, ok)
	}
}

var randomByte = []byte("2342sdfsdf124sdafasef3dsf2r233333dsfds1334qt")

func randomByteInRandomLength(length int) []byte {
	n := rand.Intn(length)
	res := make([]byte, n)
	for i := 0; i < len(res); i++ {
		res[i] = randomByte[rand.Intn(len(randomByte))]
	}
	return res[:]
}
