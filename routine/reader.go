package routine

import (
	"context"
	"github.com/PatchouliG/wisckey-db/lsm"
)

type ReadWorker struct {
	lsm          *lsm.Lsm
	requestChan  chan readRequest
	responseChan chan ReadResponse
	context      context.Context
}

func NewReaderWork() *ReadWorker {
	panic("")
}

func (rw *ReadWorker) routine() {
	// todo
	//for {
	//
	//}

}
