
include_directories(/usr/local/tars/cpp/include)

gen_server(Base StorageServer tars)

add_dependencies(StorageServer raft)

target_link_libraries(StorageServer storage raft rocksdb snappy)
