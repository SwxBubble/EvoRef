add_library(index STATIC adaptive_odess_subfeature_index.cpp)
target_link_libraries(index glog gflags feature)
