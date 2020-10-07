package lsm

const (
	level0FileNumber = 4
	level1FileNumber = 10
	// level n sstable file number = (n-1)*expand rate
	expandRate = 10
)

// dir store all db file, include sstable metadata logfile e.g
var dbDir string
