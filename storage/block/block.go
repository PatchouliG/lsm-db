package block

import "encoding/binary"

// block size 32Kb
// data*
// pad
// metadata :pad begin offset(uint64)
// block begin
//

// 32 Kb
const DataBlockSizeInByte = 32 * 1024
const dataBlockMetaDataSize = 4

type Writer struct {
	data     []byte
	position int
}

func NewWriter() Writer {
	return Writer{make([]byte, DataBlockSizeInByte), 0}
}

// return false if reach size limit
func (w *Writer) Write(data []byte) bool {
	if uint32(w.position)+uint32(len(data)) >= DataBlockSizeInByte-dataBlockMetaDataSize {
		return false
	}
	copy(w.data[w.position:], data)
	w.position += len(data)
	return true
}

func (w *Writer) Byte() []byte {
	binary.BigEndian.PutUint32(w.data[DataBlockSizeInByte-dataBlockMetaDataSize:], uint32(w.position))
	res := w.data
	w.data = nil
	return res
}

type Reader struct {
	data []byte
}

func NewReader(data []byte) Reader {
	return Reader{data}
}

func (r *Reader) Byte() []byte {
	size := binary.BigEndian.Uint32(r.data[DataBlockSizeInByte-dataBlockMetaDataSize:])
	return r.data[:size]
}
