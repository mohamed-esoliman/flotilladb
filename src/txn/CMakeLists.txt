add_library(flotilla_txn
  codec.cc
  txn_ops.cc
)
target_link_libraries(flotilla_txn PUBLIC flotilla_storage)
