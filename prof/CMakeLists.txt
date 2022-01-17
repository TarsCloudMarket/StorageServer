
gen_server(raft TestStorage)

add_dependencies(TestStorage StorageServer)

#target_link_libraries(TestStorage raft)


