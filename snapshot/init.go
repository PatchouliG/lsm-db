package snapshot

import "github.com/PatchouliG/wisckey-db/id"

func SetStartId(startId int64) {
	idGenerator = id.NewGenerator(startId)
}
