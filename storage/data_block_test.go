package storage

import (
	"github.com/PatchouliG/wisckey-db/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

var sizeLimit = 300

func TestDataBLockAppend(t *testing.T) {

	a := make([]byte, DataBlockSizeInByte/2)

	db := newDataBlock()
	res := db.appendData(a)
	assert.True(t, res)
	res = db.appendData(a)
	assert.False(t, res)

}
func TestDataBLockEncodeAndDecode(t *testing.T) {
	r1 := NewRecord(common.NewKey("key123sdddddddddd23"), common.NewValue("value1"))
	r2 := NewRecord(common.NewKey("key2"), common.NewValue("value21234sd32r2rsdd"))
	db := newDataBlock()
	db.appendData(r1.encode())
	db.appendData(r2.encode())
	data := db.encode()

	fromByte := newDataBlockFromByte(data)
	dataDecoded := fromByte.GetData()
	ri := NewRecordIterator(dataDecoded)
	assert.True(t, ri.hasNext())
	r1Decoded, ok := ri.next()
	assert.True(t, ok)
	assert.Equal(t, r1, r1Decoded)
	assert.True(t, ri.hasNext())
	r2Decoded, ok := ri.next()
	assert.True(t, ok)
	assert.Equal(t, r2, r2Decoded)

	assert.False(t, ri.hasNext())
}
