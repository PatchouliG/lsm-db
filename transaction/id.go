package transaction

func NewTransactionIdGenerator(lastId Id) chan Id {
	panic("")
}

type Id struct {
	i int64
}

// only for test
func MockId(id int64) Id {
	return Id{id}
}

func (id Id) cmp(i Id) int {
	panic("")
}
