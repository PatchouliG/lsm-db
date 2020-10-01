package record

// max 16Kb
type Value struct {
	s string
}

func (v Value) Value() string {
	return v.s
}

func NewValue(v string) Value {
	return Value{v}
}
