add_library(flotilla_sql
  catalog.cc
  executor.cc
  lexer.cc
  parser.cc
)
target_link_libraries(flotilla_sql PUBLIC flotilla_client)
