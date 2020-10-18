package metadata

import (
	"github.com/PatchouliG/lsm-db/lsm"
	"os"
)

const logfile = "memtable_logfile"

// own by main routine
type Metadata struct {
	current *lsm.Lsm
	file    os.File
}

// new db
func newMetadata(workDir string) *Metadata {
	panic("")
}

// exits db
func leadMetadata(workDir string) *Metadata {
	panic("")
}

func (m Metadata) saveToLogFile() {

}
