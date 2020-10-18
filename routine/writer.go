package routine

import (
	"context"
	"github.com/PatchouliG/lsm-db/lsm"
)

type WriteWorker struct {
	lsm          *lsm.Lsm
	requestChan  chan writeRequest
	responseChan chan WriteResponse
	context      context.Context
}

func NewWriteWork() *WriteWorker {
	panic("")
}

func (rw *WriteWorker) routine() {

}
