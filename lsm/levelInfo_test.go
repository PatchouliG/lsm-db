package lsm

import (
	"fmt"
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/storage/sstable"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sort"
	"testing"
)

func init() {
	gloablConfig.UseTestConfig()
}

func TestAddAndContainsAndPop(t *testing.T) {
	li := newLevelInfo()
	smd18 := newSStableMetaData(sstable.NextId(), record.NewKey("1"), record.NewKey("4"))
	smd27 := newSStableMetaData(sstable.NextId(), record.NewKey("2"), record.NewKey("6"))
	smd89 := newSStableMetaData(sstable.NextId(), record.NewKey("8"), record.NewKey("9"))
	li.addSStable(0, smd18)
	li.addSStable(0, smd27)
	li.addSStable(0, smd89)

	res := li.sstablesContainsKey(0, record.NewKey("4"))
	assert.Equal(t, 2, len(res))
	assert.Contains(t, res, smd27)
	assert.Contains(t, res, smd18)

	// level not exits
	res = li.sstablesContainsKey(1, record.NewKey("4"))
	assert.Equal(t, 0, len(res))

	// key not in range
	res = li.sstablesContainsKey(0, record.NewKey("7"))
	assert.Equal(t, 0, len(res))

}

func TestAddAndPop(t *testing.T) {
	li := newLevelInfo()
	a := newSStableMeta("a")
	b := newSStableMeta("b")
	c := newSStableMeta("c")
	d := newSStableMeta("d")
	li.addSStables(0, []*sstableMetaData{b, c, d, a})

	li.popSStable(1)

	res := li.popSStable(0)
	assert.Equal(t, 0, len(res))

	e := newSStableMeta("e")
	li.addSStable(0, e)
	res = li.popSStable(0)
	assert.Equal(t, 1, len(res))
	assert.Contains(t, res, a)

	a1 := newSStableMeta("a1")
	a2 := newSStableMeta("a2")
	a3 := newSStableMeta("a3")
	a4 := newSStableMeta("a4")
	a5 := newSStableMeta("a5")
	li.addSStables(0, []*sstableMetaData{a2, a3, a1, a4, a5})
	res = li.popSStable(0)
	assert.Equal(t, 5, len(res))
	assert.Contains(t, res, b)
	assert.Contains(t, res, c)
	assert.Contains(t, res, d)
	assert.Contains(t, res, e)
	assert.Contains(t, res, a1)

	// add 10 sstable to level 1
	li.addSStables(1, newSStableMetas(20))
	res = li.popSStable(1)
	assert.Equal(t, 10, len(res))

	li.addSStables(1, newSStableMetas(20))
	res = li.popSStable(1)
	assert.Equal(t, 20, len(res))
}

func TestLevelInfo_Get(t *testing.T) {
	li := newLevelInfo()
	a := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("a", "a")})
	b := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("b", "b")})
	c := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("c", "c")})
	d := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("d", "d")})
	e := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("e", "e")})
	f := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("f", "f")})
	g := newSStableMetaContainsRecord(t, []record.Record{record.NewRecordStr("g", "g")})
	//	 add sstable to level 0,level 1

	li.addSStables(0, []*sstableMetaData{a, b, c, d})
	li.addSStables(1, []*sstableMetaData{e, f, g})

	//	 get from level 0
	res, ok := li.get(record.NewKey("b"))
	assert.True(t, ok)
	value, ok := res.Value()
	assert.True(t, ok)
	assert.Equal(t, "b", value.Value())
	//	 get from level 1
	res, ok = li.get(record.NewKey("g"))
	assert.True(t, ok)
	value, ok = res.Value()
	assert.True(t, ok)
	assert.Equal(t, "g", value.Value())

	//	get fail
	res, ok = li.get(record.NewKey("x"))
	assert.False(t, ok)
}
func TestPopSStablesOverLap(t *testing.T) {
	li := newLevelInfo()

	a := newSStableMetaData(sstable.NextId(), record.NewKey("1"), record.NewKey("3"))
	b := newSStableMetaData(sstable.NextId(), record.NewKey("4"), record.NewKey("5"))
	c := newSStableMetaData(sstable.NextId(), record.NewKey("6"), record.NewKey("7"))
	d := newSStableMetaData(sstable.NextId(), record.NewKey("7"), record.NewKey("9"))

	e := newSStableMetaData(sstable.NextId(), record.NewKey("2"), record.NewKey("6"))
	f := newSStableMetaData(sstable.NextId(), record.NewKey("a"), record.NewKey("b"))

	li.addSStables(1, []*sstableMetaData{d, c, a, b})
	res := li.popSStableOverlap(1, []*sstableMetaData{})

	assert.Equal(t, 0, len(res))
	res = li.popSStableOverlap(1, []*sstableMetaData{f, e})

	assert.Len(t, res, 3)
	assert.Contains(t, res, a)
	assert.Contains(t, res, b)
	assert.Contains(t, res, c)

}

func TestSStableOverlap(t *testing.T) {
	a := newSStableMetaData(sstable.NextId(), record.NewKey("1"), record.NewKey("3"))
	b := newSStableMetaData(sstable.NextId(), record.NewKey("4"), record.NewKey("8"))
	res := sstableOverLap(a, b)
	assert.False(t, res)

	a = newSStableMetaData(sstable.NextId(), record.NewKey("8"), record.NewKey("9"))
	b = newSStableMetaData(sstable.NextId(), record.NewKey("2"), record.NewKey("3"))
	res = sstableOverLap(a, b)
	assert.False(t, res)

	a = newSStableMetaData(sstable.NextId(), record.NewKey("1"), record.NewKey("3"))
	b = newSStableMetaData(sstable.NextId(), record.NewKey("3"), record.NewKey("8"))
	res = sstableOverLap(a, b)
	assert.True(t, res)

	a = newSStableMetaData(sstable.NextId(), record.NewKey("4"), record.NewKey("9"))
	b = newSStableMetaData(sstable.NextId(), record.NewKey("2"), record.NewKey("5"))
	res = sstableOverLap(a, b)
	assert.True(t, res)

}

func newSStableMetas(size int) []*sstableMetaData {
	var res []*sstableMetaData
	for i := 0; i < size; i++ {
		res = append(res, newSStableMeta(fmt.Sprint(rand.Intn(324))))
	}
	return res
}

func newSStableMeta(startKey string) *sstableMetaData {
	return newSStableMetaData(sstable.NextId(), record.NewKey(startKey), record.NewKey(startKey+"end"))
}

func newSStableMetaContainsRecord(t *testing.T, records []record.Record) *sstableMetaData {
	sstw, err := sstable.NewWriter()
	sort.Slice(records, func(i, j int) bool {
		return records[i].Key().Value() < records[j].Key().Value()
	})
	assert.Nil(t, err)
	for _, r := range records {
		sstw.Write(r)
	}
	assert.Nil(t, sstw.FlushToFile())
	return newSStableMetaData(sstw.Id(), records[0].Key(), records[len(records)-1].Key())
}
