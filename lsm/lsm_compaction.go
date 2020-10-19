package lsm

import (
	"github.com/PatchouliG/lsm-db/memtable"
	"github.com/PatchouliG/lsm-db/storage/sstable"
)

// immutable means no record change
// use for build new sstable from memtable and sstable compaction
type lsmForCompaction struct {
	// level -> sstable metadata
	levelInfo *levelInfo
	memtable  *memtable.Memtable
}

func (l *lsmForCompaction) FlushMemtable() *levelInfo {
	// write memtable to level 0
	sstablesWithKeyRange := l.memtable.ToSStable()
	for _, s := range sstablesWithKeyRange {
		sstmd := newSStableMetaData(s.Id(), s.StartKey, s.EndKey)
		l.levelInfo.addSStable(0, sstmd)
	}

	l.checkAndCompactionAllLevels()
	res := l.levelInfo
	// clear
	l.memtable = nil
	l.levelInfo = nil
	return res
}

func (l *lsmForCompaction) checkAndCompactionAllLevels() {
	l.checkAndCompaction(0)
}

// compaction from level0 to highest level
func (l *lsmForCompaction) checkAndCompaction(levelNumber int) {
	sstableForCompactor := l.levelInfo.popSStable(levelNumber)
	if len(sstableForCompactor) == 0 {
		return
	}

	nextLevelSStableOverlapped := l.levelInfo.popSStableOverlap(levelNumber+1, sstableForCompactor)

	var sstableReaders []*sstable.Reader
	for _, sstableMetaData := range sstableForCompactor {
		sstableReaders = append(sstableReaders, sstable.NewReader(sstableMetaData.id))
	}
	for _, sstableMetaData := range nextLevelSStableOverlapped {
		sstableReaders = append(sstableReaders, sstable.NewReader(sstableMetaData.id))
	}

	compactionResult := sstable.BuildSStableFromReader(sstableReaders)

	var sstableResult []*sstableMetaData
	for _, reader := range compactionResult {
		sstableResult = append(sstableResult, newSStableMetaDataFromReader(reader))
	}
	l.levelInfo.addSStables(levelNumber+1, sstableResult)

	l.checkAndCompaction(levelNumber + 1)
}
