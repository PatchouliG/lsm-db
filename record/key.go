package record

import (
	"encoding/binary"
)

const SizeLimit = 1024

// max 1kb
type Key struct {
	s string
}

func NewKey(k string) Key {
	return Key{k}
}

func (k *Key) Value() string {
	return k.s
}

func NewKeyFromByte(data []byte) (Key, uint32) {
	size := binary.BigEndian.Uint32(data[:4])
	res := Key{string(data[4 : 4+size])}
	return res, 4 + size
}

func (k *Key) Encode() []byte {
	size := len(k.s)
	sizeByte := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeByte, uint32(size))
	return append(sizeByte, []byte(k.s)...)
}

type KeyWriter struct {
	data []byte
}

func newKeyWriter() KeyWriter {
	return KeyWriter{}
}

func (kw *KeyWriter) Writer(k Key) {
	kw.data = append(kw.data, k.Encode()...)
}

func (kw *KeyWriter) Byte() []byte {
	return kw.data
}

type KeyReader struct {
	data     []byte
	position uint32
}

func NewKeyReader(data []byte) KeyReader {
	return KeyReader{data, 0}
}

func (ki *KeyReader) HasNext() bool {
	if int(ki.position) == len(ki.data) {
		return false
	}
	return true
}

func (ki *KeyReader) Next() Key {
	res, size := NewKeyFromByte(ki.data[ki.position:])
	ki.position += size
	return res
}
