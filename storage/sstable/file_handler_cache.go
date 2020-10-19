package sstable

import (
	"github.com/PatchouliG/lsm-db/gloablConfig"
	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"os"
)

// todo use lru map
// thread safe
type fileHandlerCache struct {
	c *lru.Cache
}

func newFileHandlerCache() *fileHandlerCache {
	c, err := lru.New(100)
	if err != nil {
		log.WithError(err).Panic("create lru cache fail")
	}
	return &fileHandlerCache{c}
}

func (fhc *fileHandlerCache) get(id Id) *os.File {
	res, ok := fhc.c.Get(id)
	if ok {
		return res.(*os.File)
	}
	fileName := gloablConfig.SStableName(id.Id)
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		log.Panic("open sstable fail ", err)
	}
	fhc.c.Add(id, f)
	return f
}
