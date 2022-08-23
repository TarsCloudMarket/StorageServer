
include_directories(/usr/local/tars/cpp/include)

gen_server(Base StorageServer tars)

add_dependencies(StorageServer raft)

target_link_libraries(StorageServer storage raft rocksdb snappy)

if(UNIX AND NOT APPLE)
    target_link_libraries(StorageServer -static-libgcc -static-libstdc++)
endif()
