package memtable

import "sync"

var nextId int
var lock sync.Mutex

func getNextId() (id Id) {
	lock.Lock()
	lock.Unlock()
	id = Id{nextId}
	nextId++
	return
}

type Id struct {
	id int
}
