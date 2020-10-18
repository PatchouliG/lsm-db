package db

import "github.com/PatchouliG/lsm-db/record"

type SnapshotDB struct {
}

func (sdb *SnapshotDB) Get(key record.Key) (record.Record, bool) {
	panic("")
}

// range:[startKey,endKey]
func (sdb *SnapshotDB) Range(startKey record.Key, endKey record.Key) record.Iterator {
	panic("")
}
