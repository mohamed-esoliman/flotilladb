add_executable(sanity_test sanity_test.cc)
target_link_libraries(sanity_test GTest::gtest_main)
gtest_discover_tests(sanity_test)
