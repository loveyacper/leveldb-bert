# Just amuse myself.

## 移植历程

- [x] 跳表实现: [skiplist](leveldb/db/skiplist.go)

     - [x] 支持读写并发的实现，不支持删除

     - [x] 跳表迭代器

- [x] util相关1: 包括env,varint,status等等

- [x] memtable实现: [memtable](leveldb/db/memtable.go)

     - [x] internal key, memtable key

     - [x] 插入/删除/查找逻辑

     - [x] 迭代器

- [x] util相关2: 包括log reader/writer,提供对manifest和wal文件的存储格式及读写支持

- [x] ldb实现

     - [x] table文件格式及构建

     - [ ] 读取相关，lru缓存，bloom过滤器，待做

- [x] db实现: [db](leveldb/db/db.go)

     - [x] 初步与memtable集成，纯内存存储

     - [x] 支持wal，能够从wal文件恢复memtable数据

     - [x] 初步支持version set, 支持minor compaction

     - [ ] MUCH TODO

