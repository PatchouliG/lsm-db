package storage

import "encoding/binary"

// block size 32Kb
// data
// pad
// metadata :pad begin offset(uint64)
// block begin
//

// 32 Kb
const DataBlockSizeInByte = 32 * 1024
const dataBlockMetaDataSize = 4

type dataBlock struct {
	data []byte
	size uint32
}

func newDataBlock() dataBlock {
	return dataBlock{}
}

//return false if reach size limit
func (fb *dataBlock) appendData(data []byte) bool {
	if fb.size+uint32(len(data)) >= DataBlockSizeInByte-dataBlockMetaDataSize {
		return false
	}
	fb.data = append(fb.data, data...)
	fb.size = uint32(len(fb.data))
	return true
}

// always return block size(DataBlockSizeInByte)
func (fb *dataBlock) encode() (res []byte) {
	res = make([]byte, DataBlockSizeInByte)
	copy(res, fb.data)
	binary.BigEndian.PutUint32(res[DataBlockSizeInByte-4:], fb.size)
	return
}

func newDataBlockFromByte(data []byte) dataBlock {
	res := dataBlock{data, 0}
	res.size = binary.BigEndian.Uint32(data[DataBlockSizeInByte-4:])
	return res
}

func (fb *dataBlock) GetData() []byte {
	return fb.data[:fb.size]
}
