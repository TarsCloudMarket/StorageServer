
include_directories(/usr/local/tars/cpp/thirdparty/include/gtest/include)

gen_server(Storage storage-test)

target_link_libraries(storage-test storage raft rafttest gtest rocksdb snappy)



