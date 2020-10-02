package block

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataBLockAppend(t *testing.T) {

	a := make([]byte, DataBlockSizeInByte/2)

	w := NewWriter()
	res := w.Write(a)
	assert.True(t, res)
	res = w.Write(a)
	assert.False(t, res)

}
func TestDataBLockEncodeAndDecode(t *testing.T) {
	r1 := record.NewRecord(record.NewKey("key123sdddddddddd23"), record.NewValue("value1"))
	r2 := record.NewRecord(record.NewKey("key2"), record.NewValue("value21234sd32r2rsdd"))
	w := NewWriter()
	res := w.Write(r1.Encode())
	assert.True(t, res)
	res = w.Write(r2.Encode())
	assert.True(t, res)
	data := w.Byte()

	assert.Equal(t, DataBlockSizeInByte, len(data))

	reader := NewReader(data)
	dataDecoded := reader.Byte()
	ri := record.NewRecordReader(dataDecoded)
	assert.True(t, ri.HasNext())
	r1Decoded, ok := ri.Next()
	assert.True(t, ok)
	assert.Equal(t, r1, r1Decoded)
	assert.True(t, ri.HasNext())
	r2Decoded, ok := ri.Next()
	assert.True(t, ok)
	assert.Equal(t, r2, r2Decoded)

	assert.False(t, ri.HasNext())
}
