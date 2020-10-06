package memtable

import "github.com/PatchouliG/wisckey-db/id"

type Config struct {
	NextId     int64
	LogfileDir string
}

// must call before run
func setConfig(config Config) {
	idGenerator = id.NewGenerator(config.NextId)
	logFileOutPutDir = config.LogfileDir
}
