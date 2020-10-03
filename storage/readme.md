# storage
文件存储相关 包括 sstable 和log file
sstable 文件内部格式 写入，读取，合并
log file文件(由memtable转化),写入，读取（生成memtable）,与sstable合并

写缓存 (not implement),看profile后再做