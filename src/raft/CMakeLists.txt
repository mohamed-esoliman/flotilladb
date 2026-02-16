add_library(flotilla_raft
  raft.cc
  raft_storage.cc
  wire.cc
)
target_link_libraries(flotilla_raft PUBLIC flotilla_common)
