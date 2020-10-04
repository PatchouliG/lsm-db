package snapshot

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewTransactionIdGenerator(t *testing.T) {
	f := NewTransactionIdGenerator(0)
	last := <-f
	for i := 0; i < 10; i++ {
		next := <-f
		assert.True(t, next.Cmp(last) > 0)
	}
}
