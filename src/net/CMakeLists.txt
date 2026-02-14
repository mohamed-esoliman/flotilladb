add_library(flotilla_net
  frame.cc
  messages.cc
  socket.cc
  tcp_server.cc
)
target_link_libraries(flotilla_net PUBLIC flotilla_common)
