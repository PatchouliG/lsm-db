package record

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	k1 := NewKey("test123424423")
	data := k1.Encode()
	k2, _ := NewKeyFromByte(data)
	assert.Equal(t, k1.Value(), k2.Value())
}

func TestKeyReaderAndWriter(t *testing.T) {
	k := NewKey("key_test")
	kw := NewKeyWriter()
	number := 10
	for i := 0; i < number; i++ {
		kw.Writer(k)
	}
	data := kw.Byte()
	kr := NewKeyReader(data)
	count := 0
	for kr.HasNext() {
		r := kr.Next()
		count += 1
		assert.Equal(t, k, r)
	}
	assert.Equal(t, number, count)
}
