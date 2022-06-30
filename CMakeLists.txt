cmake_minimum_required(VERSION 3.14)

project(BPTREE)
message(STATUS "Build bptree")

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose Release or Debug" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake;${CMAKE_MOUDLE_PATH}")

option(BPTREE_BUILD_TEST "build test" ON)
option(BPTREE_BUILD_EXAMPLE "build example" ON)

file(GLOB_RECURSE BPTREE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
set(BPTREE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

add_library(BPTREE STATIC ${BPTREE_SOURCES})
target_include_directories(BPTREE PUBLIC ${BPTREE_INCLUDE_DIR})

add_library(LEVELDB STATIC IMPORTED)
set_target_properties(LEVELDB PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb/lib/libleveldb.a" 
IMPORTED_LINK_INTERFACE_LIBRARIES pthread)
target_include_directories(LEVELDB INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/leveldb/include")


if (NOT TARGET spdlog)
  include(spdlog)
endif()

include(crc32c)

if (NOT TARGET MPMCQueue)
  include(mpmcqueue)
endif()

if (NOT TARGET fmt)
  include(fmt)
endif()

if (NOT TARGET gflags)
include(gflags)
endif()

target_link_libraries(BPTREE PUBLIC spdlog::spdlog_header_only crc32 MPMCQueue fmt::fmt)

if ((NOT TARGET gtest_main) AND BPTREE_BUILD_TEST)
  include(googletest)
endif()

if (BPTREE_BUILD_TEST)
  message(STATUS "bptree build test")
  file(GLOB TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cc")
  add_executable(bptree_test ${TEST_SOURCES})
  target_link_libraries(bptree_test gtest_main BPTREE spdlog::spdlog_header_only crc32 MPMCQueue fmt::fmt)
  include(GoogleTest)
  gtest_discover_tests(bptree_test)
endif()

if (BPTREE_BUILD_EXAMPLE)
  message(STATUS "bptree build example")
  aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/example" BPEXAMPLES)

  foreach(each ${BPEXAMPLES})
    string(REGEX MATCHALL "[0-9A-Za-z_]*.cc" tmp ${each})
    string(REGEX REPLACE ".cc" "_out" nn ${tmp})
    add_executable(${nn} ${each})
    target_link_libraries(${nn} BPTREE spdlog::spdlog_header_only LEVELDB gflags)
  endforeach(each)
endif()