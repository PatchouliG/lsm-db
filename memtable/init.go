package memtable

type Config struct {
	NextId     int
	LogfileDir string
}

// must call before run
func setConfig(config Config) {
	nextId = config.NextId
	logFileOutPutDir = config.LogfileDir
}
