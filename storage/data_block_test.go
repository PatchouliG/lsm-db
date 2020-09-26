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
	r1 := NewRecord(common.NewKey("key1"), common.NewValue("value1"))
	r2 := NewRecord(common.NewKey("key2"), common.NewValue("value2"))
	db := newDataBlock()
	db.appendData(r1.encode())
	db.appendData(r2.encode())
	data := db.encode()

	fromByte := newDataBlockFromByte(data)
	dataDecoded := fromByte.GetData()
	r1Decoded, size := NewRecordFromByte(dataDecoded)
	assert.Equal(t, r1Decoded, r1)
	r2Decoded, size := NewRecordFromByte(dataDecoded[size:])
	assert.Equal(t, r2Decoded, r2)
}
