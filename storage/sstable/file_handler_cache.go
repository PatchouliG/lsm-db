package sstable

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

// todo use lru map
// thread safe
type fileHandlerCache struct {
	m    map[Id]*os.File
	lock sync.Mutex
}

func newFileHandlerCache() *fileHandlerCache {
	return &fileHandlerCache{make(map[Id]*os.File), sync.Mutex{}}
}

func (fhc *fileHandlerCache) get(id Id) *os.File {
	fhc.lock.Lock()
	defer fhc.lock.Unlock()
	res, ok := fhc.m[id]
	if ok {
		return res
	}
	fileName := gloablConfig.SStableName(id.Id)
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		log.Panic("open sstable fail ", err)
	}
	fhc.m[id] = f
	return f
}
