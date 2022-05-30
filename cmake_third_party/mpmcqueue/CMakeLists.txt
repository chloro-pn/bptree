cmake_minimum_required(VERSION 3.14)

project(mpmcqueue)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB_RECURSE QUEUE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h")
set(QUEUE_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# how to build a header-only thiry-party target ? 
add_library(mpmcqueue INTERFACE)
target_sources(mpmcqueue INTERFACE ${QUEUE_SOURCES})
target_include_directories(mpmcqueue INTERFACE ${QUEUE_INCLUDE_DIR})
