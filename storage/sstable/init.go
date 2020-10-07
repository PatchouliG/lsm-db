package sstable

import "github.com/PatchouliG/wisckey-db/id"

type Config struct {
	StartId int64
}

func SetConfig(config Config) {
	idGenerator = id.NewGenerator(config.StartId)
}
