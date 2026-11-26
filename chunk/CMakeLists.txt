add_library(chunker STATIC fast_cdc.cpp)
target_link_libraries(chunker glog gflags)
