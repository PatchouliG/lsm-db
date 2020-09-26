package common

type sstable interface {
	Get(key string) (value string, ok bool, err error)
	Merge(highLevelSStable []sstable) (sstable sstable)
	//RefCountInc()
	//RefCountDesc()
}

type Key string

func NewKey(k string) Key {
	return Key(k)
}

type Value string

func NewValue(v string) Value {
	return Value(v)
}

//todo use record
type entry struct {
	key Key
	//seperate key value ,to value position
	value Value
}

func (e entry) size() int {
	return len(e.key) + len(e.value)
}

type sortedEntryList struct {
	size      int
	entryList []entry
}

func (sel sortedEntryList) Get(key string) (value string, ok bool, err error) {
	panic("implement me")
}

func (sel sortedEntryList) Merge(highLevelSStable []sstable) (sstable sstable) {
	panic("implement me")
}

func newSortedEntryList() sortedEntryList {
	// todo init size
	return sortedEntryList{entryList: make([]entry, 1024)}
}

func (sel sortedEntryList) addEntry(entry entry) error {
	sel.size += entry.size()
	// todo
	panic("error")
}

//return first element less or equal than Key
func (sel sortedEntryList) binarySearch(key string) (position int, entry entry) {
	panic("")
}
