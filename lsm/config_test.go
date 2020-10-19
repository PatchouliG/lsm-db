package lsm

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestSStableSizs(t *testing.T) {
	l0 := sstableFileNumberLimit(0)
	l1 := sstableFileNumberLimit(1)
	l5 := sstableFileNumberLimit(5)
	assert.Equal(t, 4, l0)
	assert.Equal(t, 10, l1)
	assert.Equal(t, 100000, l5)
}
