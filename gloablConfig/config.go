package gloablConfig

import (
	"github.com/PatchouliG/lsm-db/id"
	"io/ioutil"
	"path"
)

var GlobalConfig Config

type Config struct {
	SStableStartId  int64
	MemtableStartId int64
	LsmStartId      int64
	SnapshotId      int64
	WorkDir         string
}

func UseTestConfig() {
	dir, err := ioutil.TempDir("", "sstable_test")
	if err != nil {
		panic(err)
	}

	GlobalConfig = Config{0, 0, 0, 0, dir}
}

func SStableName(id id.Id) string {
	return path.Join(GlobalConfig.WorkDir, "sstable_"+id.String())
}

func LogFileName(id id.Id) string {
	return path.Join(GlobalConfig.WorkDir, "memtable_"+id.String()+"_logFile")
}
