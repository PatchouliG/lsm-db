package db

import (
	"github.com/PatchouliG/lsm-db/record"
	"github.com/PatchouliG/lsm-db/transaction"
)

type DB struct {
}

func OpenDB(dirName string) *DB {
	panic("")
}
func (db *DB) close() {
	//	save all to file and stop
}

func (db *DB) Get(key record.Key) (record.Record, bool) {
	panic("")
}
func (db *DB) GetDBSnapshot(r record.Record, id transaction.Id) (*SnapshotDB, error) {
	panic("")
}

func (db *DB) Put(r record.Record, id transaction.Id) {
	panic("")
}

// delete multiple is ok
// todo need test delete multiple
func (db *DB) Delete(k record.Key, id transaction.Id) {
	panic("")
}
