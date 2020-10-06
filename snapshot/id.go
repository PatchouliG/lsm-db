package snapshot

import "github.com/PatchouliG/wisckey-db/id"

//func NewTransactionIdGenerator(start int) chan Id {
//	res := make(chan Id)
//	go func() {
//		current := Id{i: int64(start)}
//		for {
//			res <- current
//			current = current.next()
//		}
//	}()
//	return res
//}

var idGenerator *id.Generator

type Id struct {
	id.Id
}

func NextId() Id {
	return Id{idGenerator.Next()}
}

func (id *Id) Cmp(i Id) int64 {
	return id.Id.Cmp(i.Id)
}
