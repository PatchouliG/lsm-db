package lsm

import "github.com/PatchouliG/lsm-db/id"

// for snapshot
type Id struct {
	id.Id
}

var idGenerator *id.Generator

func NextId() Id {
	return Id{idGenerator.Next()}
}
