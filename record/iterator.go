package record

type Iterator interface {
	Next() (Record, bool)
}
