package memtable

import "github.com/PatchouliG/wisckey-db/id"

type Config struct {
	NextId int64
}

// must call before run
func SetConfig(config Config) {
	idGenerator = id.NewGenerator(config.NextId)
}
