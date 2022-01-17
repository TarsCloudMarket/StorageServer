
gen_lib(raft storage tars)

add_dependencies(storage raft rafttest)
