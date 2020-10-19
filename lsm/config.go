package lsm

import "math"

const (
	level0FileNumber = 4
)

// dir store all db file, include sstable metadata logfile e.g
var dbDir string

// 0->4
// n->10**n
func sstableFileNumberLimit(levelNumber int) int {
	if levelNumber == 0 {
		return level0FileNumber
	}
	return int(math.Pow10(levelNumber))

}
