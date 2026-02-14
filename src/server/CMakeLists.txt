add_library(flotilla_server
  kv_service.cc
)
target_link_libraries(flotilla_server PUBLIC flotilla_storage flotilla_net)

add_executable(flotilladb main.cc)
target_link_libraries(flotilladb flotilla_server)
