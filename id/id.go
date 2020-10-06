package id

import (
	"strconv"
	"sync"
)

// thread safe
type Generator struct {
	nextId Id
	lock   sync.Mutex
}

func NewGenerator(start int64) *Generator {
	return &Generator{Id{start}, sync.Mutex{}}
}

func (g *Generator) Next() Id {
	g.lock.Lock()
	g.lock.Unlock()

	res := g.nextId
	g.nextId.i++
	return res
}

type Id struct {
	i int64
}

func (id Id) String() string {
	return strconv.FormatInt(id.i, 10)
}

func (id Id) Cmp(a Id) int64 {
	return id.i - a.i
}
