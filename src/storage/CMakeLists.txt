add_library(flotilla_storage
  bloom.cc
  db.cc
  manifest.cc
  memtable.cc
  merging_iterator.cc
  sstable.cc
  wal.cc
)
target_link_libraries(flotilla_storage PUBLIC flotilla_common)
