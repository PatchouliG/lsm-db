package sstable

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

func TestSstableWriteAndFind(t *testing.T) {
	file := createTestFileName(t)
	err := createSStable(t, file)
	assert.Nil(t, err)

	sstf := NewReader(file.Name())

	// find by record
	k := record.NewKey(strconv.Itoa(10000000064))
	res, found := sstf.Find(k)
	assert.True(t, found)
	assert.Equal(t, res.Key(), k)

	v, ok := res.Value()
	assert.True(t, ok)
	assert.Equal(t, "value_10000000064", v.Value())

	// not found
	_, found = sstf.Find(record.NewKey("not exits"))
	assert.False(t, found)

	// odd number won't found
	_, found = sstf.Find(record.NewKey("171"))
	assert.False(t, found)

}

func TestReader_Next(t *testing.T) {
	file := createTestFileName(t)
	err := createSStable(t, file)
	assert.Nil(t, err)

	sstr := NewReader(file.Name())

	checkKV(t, sstr)

}

// test sstable file is in order after compaction
func TestOrderAfterCompaction(t *testing.T) {
	// low level sstable, key range overlap with high level
	a, aNumber := generateOrderedSStableFile(t, 1, 7)
	// high level sstable,key range not overlap with each other
	b, bNumber := generateOrderedSStableFile(t, 2, 5)
	c, cNumber := generateOrderedSStableFile(t, 7, 8)

	sum := aNumber + bNumber + cNumber

	fc := randFileNameGenerator(t)
	res := Compaction([]*Reader{a, b, c}, fc)
	sumOfCompactionOutput := 0

	for _, f := range res {
		sumOfCompactionOutput += checkOrder(t, f)
	}
	assert.Equal(t, sum, sumOfCompactionOutput)
}

func createTestFileName(t *testing.T) *os.File {
	file, err := ioutil.TempFile("", "TestSstableWriteAndRead")
	assert.Nil(t, err)
	return file
}

// key range: odd number from 10000000000
func createSStable(t *testing.T, file *os.File) error {
	// build sstable file
	sstw, err := NewSStableWriter(file.Name())
	assert.Nil(t, err)

	blockCount := 10000000000
	for {
		// value + count
		r := record.NewRecord(record.NewKey(strconv.Itoa(blockCount)), record.NewValue("value_"+strconv.Itoa(blockCount)))
		ok := sstw.Write(r)
		if !ok {
			break
		}
		blockCount += 2
	}

	err = sstw.FlushToFile()
	assert.Nil(t, err)
	return err
}

// check value is "value_"+key
func checkKV(t *testing.T, sstr *Reader) {
	for {
		r, ok := sstr.Next()
		if !ok {
			break
		}
		value, ok := r.Value()
		assert.True(t, ok)
		assert.Equal(t, "value_"+r.Key().Value(), value.Value())
	}
}

func randFileNameGenerator(t *testing.T) chan string {
	res := make(chan string)
	go func() {
		for {
			file := createTestFileName(t)
			res <- file.Name()
		}
	}()
	return res
}

// key range [startKey,endKey]
// return full sstable and record count
func generateOrderedSStableFile(t *testing.T, startKeyFirstLetter int, endKeyFirstLetter int) (*Reader, int) {
	file := createTestFileName(t)

	sstw, err := NewSStableWriter(file.Name())
	assert.Nil(t, err)

	var keys []string
	// make sure generate a full sstable
	for i := 0; i < 75000; i++ {
		// key format: key_i
		a := strconv.Itoa(rand.Intn(endKeyFirstLetter-startKeyFirstLetter+1)) + "_" + strconv.Itoa(i)
		keys = append(keys, a)
	}
	// sort key
	sort.Strings(keys)

	for i, key := range keys {
		ok := sstw.Write(record.NewRecordStr(key, "value_"+key))
		if !ok {
			err = sstw.FlushToFile()
			assert.Nil(t, err)
			return NewReader(file.Name()), i
		}
	}
	t.Fatal("key set size is not enough fill up sstable")
	panic("")
}

// return record number
func checkOrder(t *testing.T, r *Reader) int {
	var lastRecord *record.Record = nil
	count := 0
	for {
		r, ok := r.Next()

		if !ok {
			return count
		}

		count += 1
		if lastRecord != nil {
			assert.LessOrEqual(t, lastRecord.Key().Value(), r.Key().Value())
		}
		lastRecord = &r
	}
}
