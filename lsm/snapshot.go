package lsm

import (
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
)

// for snapshot read
type Snapshot struct {
	//read only
	*oneMemtable
	// latest transaction id in snapshot
	id transaction.Id
}

// todo check memtable by transaction id
func (s *Snapshot) Get(key record.Key) record.Iterator {
	panic("")
}

// get all records which key in [startKey,endKey)
func (s *Snapshot) RangeQuery(startKey record.Key, endKey record.Key) record.Iterator {
	panic("")
}
