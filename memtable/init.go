package memtable

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/id"
)

func init() {
	idGenerator = id.NewGenerator(gloablConfig.GlobalConfig.MemtableStartId)
}
