add_library(flotilla_common
  crc32.cc
  status.cc
)
target_include_directories(flotilla_common PUBLIC ${CMAKE_SOURCE_DIR}/src)
