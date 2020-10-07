package sstable

import "github.com/PatchouliG/wisckey-db/id"

var nextId int64

type Id struct {
	Id id.Id
}

func NextId() Id {
	return Id{idGenerator.Next()}
}

func (id Id) Cmp(i Id) int64 {
	return id.Id.Cmp(i.Id)
}

var idGenerator *id.Generator
