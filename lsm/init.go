package lsm

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	"github.com/PatchouliG/lsm-db/id"
)

// must call
func init() {
	idGenerator = id.NewGenerator(gloablConfig.GlobalConfig.LsmStartId)
}
