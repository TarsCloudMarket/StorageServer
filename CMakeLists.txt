cmake_minimum_required(VERSION 3.16)

project(StorageServer)

option(TARS_MYSQL "option for mysql" ON)
option(TARS_SSL "option for ssl" OFF)
option(TARS_HTTP2 "option for http2" OFF)

if(WIN32)
    include (c:\\tars\\cpp\\makefile\\tars-tools.cmake)
else()
    include (/usr/local/tars/cpp/makefile/tars-tools.cmake)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frtti")

# Auto-detect installed raft version under /usr/local/tars/raft_*
# Override by passing -DTARS_RAFT_PATH=/path/to/raft to cmake
if(NOT TARS_RAFT_PATH)
    file(GLOB _raft_dirs LIST_DIRECTORIES true "/usr/local/tars/raft_*")
    if(_raft_dirs)
        list(SORT _raft_dirs)
        list(GET _raft_dirs -1 TARS_RAFT_PATH)
    endif()
endif()
if(NOT TARS_RAFT_PATH OR NOT EXISTS "${TARS_RAFT_PATH}")
    message(FATAL_ERROR "Raft library not found. Install raft under /usr/local/tars/raft_* or set -DTARS_RAFT_PATH=<path>")
endif()
message(STATUS "Using TARS_RAFT_PATH: ${TARS_RAFT_PATH}")

include_directories(/usr/local/tars/cpp/thirdparty/include)
link_directories(/usr/local/tars/cpp/thirdparty/lib)
link_directories(/usr/local/tars/cpp/thirdparty/lib64)

include_directories(tars)
include_directories(${TARS_RAFT_PATH}/include)
include_directories(${TARS_RAFT_PATH}/include/rafttest)
include_directories(lib)
include_directories(src)

include_directories(${TARS_RAFT_PATH}/thirdparty/include)
link_directories(${TARS_RAFT_PATH}/lib)
link_directories(${TARS_RAFT_PATH}/thirdparty/lib)

add_subdirectory(tars)
add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(prof)

add_custom_target(start COMMAND sh ${PROJECT_SOURCE_DIR}/scripts/start.sh ${PROJECT_BINARY_DIR})
add_dependencies(start "StorageServer")
