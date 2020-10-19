package lsm

import (
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/storage/sstable"
	log "github.com/sirupsen/logrus"
)

// own by lsm
type sstableMetaData struct {
	id       sstable.Id
	startKey record.Key
	endKey   record.Key
	// todo remove
	refCount int
}

func newSStableMetaDataFromReader(keyRange *sstable.ReaderWithKeyRange) *sstableMetaData {
	return newSStableMetaData(keyRange.Id(), keyRange.StartKey, keyRange.EndKey)
}

func newSStableMetaData(id sstable.Id, startKey record.Key, endKey record.Key) *sstableMetaData {
	if startKey.Value() > endKey.Value() {
		log.WithField("start key", startKey).WithField("end key", endKey).
			Panic("sstable start key can't more than endkey")
	}
	return &sstableMetaData{id, startKey, endKey, 1}
}

// remove  sstable file if ref count is 0
func (sstm *sstableMetaData) decRef() {
	sstm.refCount--
	if sstm.refCount == 0 {
		sstm.deleteSStable()
	}
}

// todo use for gc
func (sstm *sstableMetaData) deleteSStable() {
	panic("")
}
