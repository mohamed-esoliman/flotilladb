add_library(flotilla_server
  cluster_config.cc
  kv_service.cc
  raft_node.cc
)
target_link_libraries(flotilla_server PUBLIC flotilla_storage flotilla_raft flotilla_net)

add_executable(flotilladb main.cc)
target_link_libraries(flotilladb flotilla_server)
