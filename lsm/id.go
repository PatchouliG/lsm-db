package lsm

import "github.com/PatchouliG/wisckey-db/id"

type sstableId struct {
	id.Id
}

var sstableIdGenerator *id.Generator

func NextSSTableId() sstableId {
	return sstableId{sstableIdGenerator.Next()}
}

type Id struct {
	id.Id
}

var idGenerator *id.Generator

func NextId() Id {
	return Id{idGenerator.Next()}
}
