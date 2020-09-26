package lsm

type DB interface {
	// return until flush to disk
	Put(key string, value string) error
	Get(Key string) (value string, exit bool, err error)
	Delete(Key string) error
	Close() error
}
