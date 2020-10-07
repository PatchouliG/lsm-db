package gloablConfig

import (
	"github.com/PatchouliG/wisckey-db/id"
	"github.com/spf13/viper"
	"path"
)

// set to tmp dir if test
var WorkDir = viper.GetString("WorkDir")

func SStableName(id id.Id) string {
	return path.Join(WorkDir, "sstable_"+id.String())
}
