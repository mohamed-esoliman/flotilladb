add_library(flotilla_client
  client.cc
)
target_link_libraries(flotilla_client PUBLIC flotilla_net)

add_executable(flotilla-cli cli_main.cc)
target_link_libraries(flotilla-cli flotilla_client)
