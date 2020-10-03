package sstable

import (
	"github.com/PatchouliG/wisckey-db/record"
)

type RecordIterator interface {
	Next() (record.Record, bool)
}
