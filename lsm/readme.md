when sstable write fail (log file size limit)

1. create new OneMemtable with two memtable,mutable(new) and immutable(old)
2. use the new OneMemtable as latest OneMemtable
3. write the new record to new mutable memtable
4. create write routine to save the old memtable to sstable
5. when write finish,create new OneMemtable (only new memtable and new sstables),update latest OneMemtable
6. discard old memtable, old sstable (if no snapshot)