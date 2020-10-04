package snapshot

func NewTransactionIdGenerator(start int) chan Id {
	res := make(chan Id)
	go func() {
		current := Id{i: int64(start)}
		for {
			res <- current
			current = current.next()
		}
	}()
	return res
}

type Id struct {
	i int64
}

func (id Id) next() Id {
	return Id{id.i + 1}
}

// only for test
func MockId(id int64) Id {
	return Id{id}
}

func (id *Id) Cmp(i Id) int {
	return int(id.i - i.i)
}
