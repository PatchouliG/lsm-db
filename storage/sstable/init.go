package sstable

import "github.com/PatchouliG/wisckey-db/id"

type Config struct {
	startId int64
}

func setConfig(config Config) {
	idGenerator = id.NewGenerator(config.startId)
}
