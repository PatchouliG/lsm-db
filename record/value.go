package record

// max 16Kb
type Value string

func NewValue(v string) Value {
	return Value(v)
}
