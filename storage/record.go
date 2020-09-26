package storage

import (
	"encoding/binary"
	"errors"
	"github.com/PatchouliG/wisckey-db/common"
)

type Record struct {
	key common.Key
	// empty if delete is true
	value  common.Value
	delete bool
}

func NewRecord(key common.Key, value common.Value) Record {
	return Record{key, value, false}
}

func NewDeleteRecord(key common.Key) Record {
	return Record{key, "", true}
}

// key_size uint32
// key_data []byte
// is_delete one byte 0 false 1 true
// value_size uint32
// value_data []byte
func (r *Record) encode() (res []byte) {

	keySize := make([]byte, 4)
	keyData := []byte(r.key)

	binary.BigEndian.PutUint32(keySize, uint32(len(keyData)))

	res = append(res, keySize...)
	res = append(res, keyData...)

	var isDelete byte = 0
	if r.isDeleted() {
		isDelete = 1
		res = append(res, isDelete)
		return
	}
	res = append(res, isDelete)

	valueSize := make([]byte, 4)
	valueData := []byte(r.value)

	binary.BigEndian.PutUint32(valueSize, uint32(len(valueData)))

	res = append(res, valueSize...)
	res = append(res, valueData...)
	return
}

func NewRecordFromByte(data []byte) (r Record, byteSize int) {

	offset := uint32(4)
	keySize := binary.BigEndian.Uint32(data[:offset])
	keyValue := string(data[offset : offset+keySize])
	r.key = common.NewKey(keyValue)

	offset = offset + keySize

	if data[offset] == 1 {
		r.delete = true
		byteSize = int(offset)
	} else {
		r.delete = false
		offset += 1
		valueSize := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		r.value = common.NewValue(string(data[offset : offset+valueSize]))
		byteSize = int(offset + valueSize)
	}
	return
}

func (r *Record) getKey() common.Key {
	return r.key
}

func (r *Record) isDeleted() bool {
	return r.delete
}

func (r *Record) getValue() (common.Value, error) {
	if r.delete {
		return "", errors.New("is deleted")
	}
	return r.value, nil
}

type RecordIterator struct {
	data     []byte
	position int
}

func NewRecordIterator(data []byte) RecordIterator {
	return RecordIterator{data, 0}
}

func (ri *RecordIterator) next() (Record, error) {
	if !ri.hasNext() {
		return Record{}, errors.New("no more record")
	}
	res, size := NewRecordFromByte(ri.data[ri.position:])
	ri.position += size
	return res, nil
}
func (ri *RecordIterator) hasNext() bool {
	return ri.position < len(ri.data)
}
