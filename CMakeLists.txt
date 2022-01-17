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

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frtti")

include_directories(/usr/local/tars/cpp/thirdparty/include)
link_directories(/usr/local/tars/cpp/thirdparty/lib)

include_directories(tars)
include_directories(libraft)
include_directories(libraft/src)
include_directories(lib)
include_directories(src)

include_directories(${CMAKE_BINARY_DIR}/src/rocksdb/include)
link_directories(${CMAKE_BINARY_DIR}/src/rocksdb/lib)
link_directories(${CMAKE_BINARY_DIR}/src/rocksdb/lib64)

include_directories(${CMAKE_BINARY_DIR}/src/snappy/include)
link_directories(${CMAKE_BINARY_DIR}/src/snappy/lib)
link_directories(${CMAKE_BINARY_DIR}/src/snappy/lib64)

add_subdirectory(libraft)
add_subdirectory(tars)
add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(prof)

add_custom_target(start COMMAND sh ${PROJECT_SOURCE_DIR}/scripts/start.sh ${PROJECT_BINARY_DIR})
add_dependencies(start "StorageServer")
