package sstable

import (
	"github.com/PatchouliG/wisckey-db/gloablConfig"
	"github.com/PatchouliG/wisckey-db/id"
)

type Config struct {
	StartId int64
}

func init() {
	idGenerator = id.NewGenerator(gloablConfig.GlobalConfig.SStableStartId)
}
