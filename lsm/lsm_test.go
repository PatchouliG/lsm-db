package lsm

import (
	"fmt"
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func init() {
	gloablConfig.UseTestConfig()
}

var readPercent = 10

func TestLsmDeleteAndUpdate(t *testing.T) {
	lsm := NewLsm()
	size := 100000
	prefix := "test_update_and_delete"
	// insert record
	for next := 0; ; next++ {
		if next >= size {
			break
		}
		r := newRecord(prefix, next)
		ok := lsm.Put(r, transaction.NextId())
		if !ok {
			//	sleep and retry
			time.Sleep(time.Second * 3)
			ok = lsm.Put(r, transaction.NextId())
			assert.True(t, ok)
		}
	}
	// update if next mod 5
	// delete if next mod 5 =1
	for next := 0; ; next++ {
		if next >= size {
			break
		}
		var r record.Record
		if next%5 == 0 {
			r = newUpdateRecord(prefix, next, "update")
		} else if next%5 == 1 {
			r = newDeleteRecord(prefix, next)
		} else {
			continue
		}
		ok := lsm.Put(r, transaction.NextId())
		if !ok {
			//	sleep and retry
			time.Sleep(time.Second * 3)
			ok = lsm.Put(r, transaction.NextId())
			assert.True(t, ok)
		}
	}
	// check get
	for next := 0; ; next++ {
		if next >= size {
			break
		}
		r := newRecord(prefix, next)
		value, _ := r.Value()
		res, ok := lsm.Get(r.Key())
		assert.True(t, ok, fmt.Sprintf("next is %d", next))
		if next%5 == 0 {
			resValue, ok := res.Value()
			assert.True(t, ok)
			assert.Equal(t, "update", resValue.Value())
		} else if next%5 == 1 {
			assert.True(t, res.IsDeleted())
		} else {
			resValue, ok := res.Value()
			assert.True(t, ok)
			assert.Equal(t, value.Value(), resValue.Value())
		}
	}
	fmt.Print(lsm.levelHeight())
}

func TestLsmPutAndGetWithMultipleGoRoutine(t *testing.T) {
	size := 1000000
	lsm := NewLsm()
	finish := make(chan struct{})
	concurrencyNumber := 3
	for i := 0; i < concurrencyNumber; i++ {
		go func(i int) {
			putAndGet(t, size/concurrencyNumber, fmt.Sprint(i), lsm)
			finish <- struct{}{}
		}(i)
	}
	for i := 0; i < concurrencyNumber; i++ {
		_ = <-finish
	}
}

func putAndGet(t *testing.T, size int, prefix string, lsm *Lsm) {
	for next := 0; ; next++ {
		if next >= size {
			break
		}
		r := newRecord(prefix, next)
		ok := lsm.Put(r, transaction.NextId())
		if !ok {
			//	sleep and retry
			time.Sleep(time.Second * 3)
			ok = lsm.Put(r, transaction.NextId())
			assert.True(t, ok)
		}

		// check get
		// todo debug
		if rand.Intn(100) > readPercent {
			continue
		}

		// get test
		recordForGet := newRecord(prefix, rand.Intn(next+1))
		res, ok := lsm.Get(recordForGet.Key())

		assert.True(t, ok)
		assert.Equal(t, recordForGet, res)

		//	get fail test
		recordNotFound := record.NewRecordStr(recordForGet.Key().Value()+"not_found", "")
		_, ok = lsm.Get(recordNotFound.Key())
		assert.False(t, ok)
	}
}
func newDeleteRecord(prefix string, i int) record.Record {
	key, _ := getKV(prefix, i)
	return record.NewDeleteRecord(record.NewKey(key))
}

func newUpdateRecord(prefix string, i int, value string) record.Record {
	key, _ := getKV(prefix, i)
	return record.NewRecordStr(key, value)
}

// use prefix to distinguish different go routine
func newRecord(prefix string, i int) record.Record {
	key, value := getKV(prefix, i)
	return record.NewRecordStr(key, value)
}

func getKV(prefix string, i int) (string, string) {
	r := rand.NewSource(int64(i))
	v := rand.New(r).Int()
	key := fmt.Sprint(v) + "_key_" + fmt.Sprint(i) + "_prefix_" + prefix
	value := "value_" + key
	return key, value
}
