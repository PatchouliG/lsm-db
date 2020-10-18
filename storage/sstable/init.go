package sstable

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/id"
)

type Config struct {
	StartId int64
}

func init() {
	idGenerator = id.NewGenerator(gloablConfig.GlobalConfig.SStableStartId)
}
