package lsm

import (
	"github.com/PatchouliG/lsm-db/id"
)

type config struct {
	lsmNextId     int64
	sstableNextId int64
	dbDir         string
}

// must call
func SetConfig(config config) {
	idGenerator = id.NewGenerator(config.lsmNextId)
	sstableIdGenerator = id.NewGenerator(config.sstableNextId)
	dbDir = config.dbDir
}
