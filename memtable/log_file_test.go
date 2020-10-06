package memtable

import (
	"github.com/PatchouliG/wisckey-db/record"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
)

func TestLogFileWriteAndRead(t *testing.T) {
	f := createTestFileName(t)

	lfw := NewLogFileWriter(f.Name())

	a := record.NewRecordStr("key1", "value1")
	b := record.NewRecordStr("key2", "value2")
	lfw.Write(a)
	lfw.Write(b)
	lfw.Flush()

	flr := newLogFileReader(f.Name())

	r, ok := flr.Next()
	assert.True(t, ok)
	assert.Equal(t, a, r)

	r, ok = flr.Next()
	assert.True(t, ok)
	assert.Equal(t, b, r)

	_, ok = flr.Next()
	assert.False(t, ok)
}

// todo add test, restore memtable from log file

func generateLogFile(t *testing.T, startKey int, endKey int) *logFileReader {
	f := createTestFileName(t)

	res := NewLogFileWriter(f.Name())
	var keys []string
	for i := 0; i < 100000; i++ {
		prefix := rand.Intn(endKey-startKey+1) + startKey
		// format key_i
		key := strconv.Itoa(prefix) + "_" + strconv.Itoa(i)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ok := res.Write(record.NewRecordStr(key, "value_"+key))
		if !ok {
			return newLogFileReader(f.Name())
		}
	}
	t.Fatal("should build a full log file ")
	panic("")
}

func createTestFileName(t *testing.T) *os.File {
	file, err := ioutil.TempFile("", "TestSstableWriteAndRead")
	assert.Nil(t, err)
	return file
}
