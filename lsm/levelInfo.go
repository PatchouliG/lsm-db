package lsm

import (
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/storage/sstable"
	"math/rand"
	"sort"
)

type levelInfo struct {
	levels map[int][]*sstableMetaData
	r      *rand.Rand
}

func newLevelInfo() *levelInfo {
	return &levelInfo{make(map[int][]*sstableMetaData), rand.New(rand.NewSource(0))}
}

func (l *levelInfo) get(key record.Key) (record.Record, bool) {
	for level := 0; ; level++ {

		// already search all levels
		if len(l.levels[level]) == 0 {
			break
		}

		sstableMetaDatas := l.sstablesContainsKey(level, key)

		if len(sstableMetaDatas) == 0 {
			continue
		}
		// order by id, check recently create sstable
		sort.Slice(sstableMetaDatas, func(i, j int) bool {
			return sstableMetaDatas[i].id.Cmp(sstableMetaDatas[j].id) > 0
		})

		for _, sstableMetaData := range sstableMetaDatas {
			sst := sstable.NewReader(sstableMetaData.id)
			res, ok := sst.Find(key)
			if ok {
				return res, ok
			}
		}
	}
	return record.Record{}, false
}

// return nil if no sstable in the level
func (l *levelInfo) sstablesContainsKey(levelNumber int, key record.Key) []*sstableMetaData {
	sstableMetaDatas := l.levels[levelNumber]
	var res []*sstableMetaData
	for _, sstableMetaData := range sstableMetaDatas {
		if key.Value() >= sstableMetaData.startKey.Value() && key.Value() <= sstableMetaData.endKey.Value() {
			res = append(res, sstableMetaData)
		}
	}
	return res
}

func (l *levelInfo) popSStableOverlap(levelNumber int, sstmd []*sstableMetaData) []*sstableMetaData {
	sstablesInLevel := l.levels[levelNumber]
	sort.Slice(sstmd, func(i, j int) bool {
		return sstmd[i].id.Cmp(sstmd[j].id) < 0
	})

	overlapSStables := make(map[int]struct{})

	for sstablesInLevelIndex := 0; sstablesInLevelIndex < len(sstablesInLevel); sstablesInLevelIndex++ {
		for j := 0; j < len(sstmd); j++ {
			if sstableOverlap(sstablesInLevel[sstablesInLevelIndex], sstmd[j]) {
				overlapSStables[sstablesInLevelIndex] = struct{}{}
			}
		}
	}
	var res, remain []*sstableMetaData

	for position, sstableMetadata := range sstablesInLevel {
		if _, ok := overlapSStables[position]; ok {
			res = append(res, sstablesInLevel[position])
		} else {
			remain = append(remain, sstableMetadata)
		}
	}

	l.sort(remain)
	l.levels[levelNumber] = remain

	return res
}

func sstableOverlap(a *sstableMetaData, b *sstableMetaData) bool {
	return !(a.endKey.Value() < b.startKey.Value() || a.startKey.Value() > b.endKey.Value())
}

// pop sstable
// return empty if size if sstable number is less than limit
func (l *levelInfo) popSStable(levelNumber int) []*sstableMetaData {
	sstableMetaDatas := l.levels[levelNumber]
	var sstableForCompaction []*sstableMetaData
	if len(sstableMetaDatas) == 0 {
		return sstableForCompaction
	}
	number := len(sstableMetaDatas) - sstableFileNumberLimit(levelNumber)

	if number <= 0 {
		return sstableForCompaction
	}

	// level0 must FIFO
	if levelNumber == 0 {
		sort.Slice(l.levels[0], func(i, j int) bool {
			return l.levels[0][i].id.Cmp(l.levels[0][j].id) < 0
		})
		res := l.levels[0][:number]
		l.levels[0] = l.levels[0][number:]
		return res
	}

	// random pop
	var index = l.r.Intn(len(sstableMetaDatas))

	var startIndex, endIndex int
	startIndex = index
	// pick sstable for compaction
	for {
		if index >= len(sstableMetaDatas) {
			index = 0
		}
		sstableForCompaction = append(sstableForCompaction, sstableMetaDatas[index])
		if len(sstableForCompaction) == number {
			endIndex = index
			break
		}
		index++
	}

	// remove sstable participate in compaction
	if endIndex >= startIndex {
		l.levels[levelNumber] = append(sstableMetaDatas[:startIndex], sstableMetaDatas[endIndex+1:]...)
	} else {
		l.levels[levelNumber] = sstableMetaDatas[endIndex+1 : startIndex]
	}

	return sstableForCompaction
}
func (l *levelInfo) addSStable(levelNumber int, data *sstableMetaData) {
	l.addSStables(levelNumber, []*sstableMetaData{data})
}

func (l *levelInfo) addSStables(levelNumber int, sstableMetaData []*sstableMetaData) {

	levelInfo := append(l.levels[levelNumber], sstableMetaData...)
	// keep sstable order by key
	l.sort(levelInfo)

	l.levels[levelNumber] = levelInfo
}

func (l *levelInfo) sort(levelInfo []*sstableMetaData) {
	sort.Slice(levelInfo, func(i, j int) bool {
		return levelInfo[i].startKey.Value() < levelInfo[j].startKey.Value()
	})
}

// -1 if no sstable
func (l *levelInfo) height() int {
	for i := 0; ; i++ {
		if len(l.levels[i]) == 0 {
			return i - 1
		}
	}
}

func (l *levelInfo) clone() *levelInfo {
	res := newLevelInfo()

	for levelNumber, sstables := range l.levels {
		var sstablesCloned []*sstableMetaData
		for _, sstableMetadata := range sstables {
			sstablesCloned = append(sstablesCloned, sstableMetadata)
		}
		res.levels[levelNumber] = sstablesCloned
	}

	return res
}
