# Roadmap

- ✅ write an in-memory key-value server
- ✅ refactor to support different kind of backends
   (Eg: in-memory or file-store based)
- 🛠️  implement bitcask based backend
  - ✅ implement insertion
  - ✅ implement get
  - ✅ implement delete
  - implement crc
- discard expired entries (aka compaction)
- implement wal
- implement raft over wal (for fault tolerance)
- support batch writes for performance
- introduce snapshotting
