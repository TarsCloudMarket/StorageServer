

gen_server(Base StorageServer tars)

target_link_libraries(StorageServer storage raft rocksdb snappy)

if(UNIX AND NOT APPLE)
    target_link_libraries(StorageServer -static-libgcc -static-libstdc++)
endif()
