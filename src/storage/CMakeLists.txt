add_library(flotilla_storage
  bloom.cc
  memtable.cc
  sstable.cc
  wal.cc
)
target_link_libraries(flotilla_storage PUBLIC flotilla_common)
