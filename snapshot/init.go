package snapshot

import (
	"github.com/PatchouliG/wisckey-db/gloablConfig"
	"github.com/PatchouliG/wisckey-db/id"
)

func init() {
	idGenerator = id.NewGenerator(gloablConfig.GlobalConfig.SnapshotId)
}
