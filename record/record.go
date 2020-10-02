package record

import (
	"crypto/md5"
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

func NewRecordStr(key string, value string) Record {
	return NewRecord(NewKey(key), NewValue(value))
}

func NewDeleteRecord(key Key) Record {
	return Record{key, Value{}, true}
}

const minRecordSize = 16 + 4 + 1

// md5 16byte
// key_size uint32
// is_delete one byte 0 false 1 true
// value_size uint32 (0 if is deleted)
// key_data []byte
// value_data []byte
func (r *Record) Encode() (res []byte) {

	keySize := make([]byte, 4)
	keyData := []byte(r.key.s)

	binary.BigEndian.PutUint32(keySize, uint32(len(keyData)))

	res = append(res, keySize...)

	var isDelete byte = 0
	if r.IsDeleted() {
		isDelete = 1
		res = append(res, isDelete)
		return
	}

	valueSize := make([]byte, 4)
	valueData := []byte(r.value.Value())

	binary.BigEndian.PutUint32(valueSize, uint32(len(valueData)))

	res = append(res, isDelete)
	res = append(res, valueSize...)
	res = append(res, keyData...)
	res = append(res, valueData...)
	md5Sum := md5.Sum(res)
	res = append(md5Sum[:], res...)
	return
}

// false if parse fail
func NewRecordFromByte(data []byte) (r Record, byteSize int, ok bool) {

	if len(data) < minRecordSize {
		ok = false
		return
	}

	offset := uint32(16)
	var md5Sum [16]byte
	copy(md5Sum[:], data[:16])

	keySize := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if data[offset] == 1 {
		r.delete = true
		byteSize = int(offset)
		ok = checkMd5Sum(data[16:byteSize-16], md5Sum)
		return
	} else {
		r.delete = false
		offset += 1
		if len(data) < int(offset+4) {
			ok = false
			return
		}
		valueSize := binary.BigEndian.Uint32(data[offset : offset+4])

		offset += 4
		if len(data) < int(offset+keySize) {
			ok = false
			return
		}
		keyValue := string(data[offset : offset+keySize])
		r.key = NewKey(keyValue)

		offset = offset + keySize
		if len(data) < int(offset+valueSize) {
			ok = false
			return
		}
		r.value = NewValue(string(data[offset : offset+valueSize]))
		byteSize = int(offset + valueSize)
	}
	ok = checkMd5Sum(data[16:byteSize], md5Sum)
	return
}
func checkMd5Sum(data []byte, sum [16]byte) bool {
	return md5.Sum(data) == sum
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

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) Write(r Record) {
	w.data = append(w.data, r.Encode()...)
}

func (w *Writer) Len() int {
	return len(w.data)
}

func (w *Writer) Byte() []byte {
	return w.data
}

type Reader struct {
	data     []byte
	position int
}

func NewRecordReader(data []byte) *Reader {
	return &Reader{data, 0}
}

func (ri *Reader) Next() (Record, bool) {
	if !ri.HasNext() {
		return Record{}, false
	}
	res, size, ok := NewRecordFromByte(ri.data[ri.position:])
	if !ok {
		return Record{}, false
	}
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
