package record

import (
	"encoding/binary"
	"fmt"
)

type Record struct {
	key Key
	// empty if delete is true
	value  Value
	delete bool
}

func NewRecord(key Key, value Value) Record {
	return Record{key, value, false}
}

func NewDeleteRecord(key Key) Record {
	return Record{key, Value{}, true}
}

// key_size uint32
// key_data []byte
// is_delete one byte 0 false 1 true
// value_size uint32
// value_data []byte
func (r *Record) Encode() (res []byte) {

	keySize := make([]byte, 4)
	keyData := []byte(r.key.s)

	binary.BigEndian.PutUint32(keySize, uint32(len(keyData)))

	res = append(res, keySize...)
	res = append(res, keyData...)

	var isDelete byte = 0
	if r.IsDeleted() {
		isDelete = 1
		res = append(res, isDelete)
		return
	}
	res = append(res, isDelete)

	valueSize := make([]byte, 4)
	valueData := []byte(r.value.Value())

	binary.BigEndian.PutUint32(valueSize, uint32(len(valueData)))

	res = append(res, valueSize...)
	res = append(res, valueData...)
	return
}

func NewRecordFromByte(data []byte) (r Record, byteSize int) {

	offset := uint32(4)
	keySize := binary.BigEndian.Uint32(data[:offset])
	keyValue := string(data[offset : offset+keySize])
	r.key = NewKey(keyValue)

	offset = offset + keySize

	if data[offset] == 1 {
		r.delete = true
		byteSize = int(offset)
	} else {
		r.delete = false
		offset += 1
		valueSize := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		r.value = NewValue(string(data[offset : offset+valueSize]))
		byteSize = int(offset + valueSize)
	}
	return
}

func (r Record) Key() Key {
	return r.key
}

func (r Record) IsDeleted() bool {
	return r.delete
}

func (r Record) Value() (Value, bool) {
	if r.delete {
		return Value{}, false
	}
	return r.value, true
}

func FirstRecord(data []byte) (Record, bool) {
	iterator := NewRecordReader(data)
	return iterator.Next()
}

type Writer struct {
	data []byte
}

func NewWriter() Writer {
	return Writer{}
}

func (w *Writer) Write(r Record) {
	w.data = append(w.data, r.Encode()...)
}

func (w *Writer) Byte() []byte {
	return w.data
}

type Reader struct {
	data     []byte
	position int
}

func NewRecordReader(data []byte) Reader {
	return Reader{data, 0}
}

func (ri *Reader) Next() (Record, bool) {
	if !ri.HasNext() {
		return Record{}, false
	}
	res, size := NewRecordFromByte(ri.data[ri.position:])
	ri.position += size
	return res, true
}

func (ri *Reader) HasNext() bool {
	return ri.position < len(ri.data)
}

func (ri *Reader) FindBy(key Key) (Record, bool) {
	lastPosition := ri.position
	defer func() { ri.position = lastPosition }()

	ri.position = 0
	for ri.HasNext() {
		r, _ := ri.Next()
		if r.key == key {
			return r, true
		}
	}
	return Record{}, false
}

func (r *Record) String() string {
	return fmt.Sprintf("{key:%s, isDelete: %t,value: %s}", r.key, r.delete, r.value)
}
