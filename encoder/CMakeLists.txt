find_package(GTest REQUIRED)
add_executable(xdelta_test xdelta_test.cpp)
target_link_libraries(xdelta_test xdelta3 gtest gtest_main glog gflags)
add_test(NAME xdelta_test COMMAND xdelta_test)
