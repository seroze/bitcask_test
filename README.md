# Roadmap

- ✅ write an in-memory key-value server
- ✅ refactor to support different kind of backends (Eg: in-memory or file-store based)
- ✅  implement basic bitcask based backend
  - ✅ implement insertion
  - ✅ implement get
  - ✅ implement delete
  - ✅ implement crc
- ✅ handle shutdown gracefully
- ✅ discard expired entries (aka compaction)
- implement wal
- implement checkpointing (to stop the wal growing indefinetely)
- support batch writes for performance (this has to be atomic, use wal)
- implement raft over wal (for fault tolerance)
- benchmark performance
