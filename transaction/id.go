package transaction

func NewTransactionIdGenerator(lastId Id) chan Id {
	panic("")
}

type Id struct {
	i int64
}

func (id Id) cmp(i Id) int {
	panic("")
}
