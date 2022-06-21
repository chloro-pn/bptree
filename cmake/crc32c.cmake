include(FetchContent)

set(CRC32_GIT_URL https://ghproxy.com/https://github.com/gityf/crc)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ext/crc32c)
  FetchContent_Declare(
    crc32c
    GIT_REPOSITORY ${CRC32_GIT_URL}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext/crc32c
  )
  
  FetchContent_MakeAvailable(crc32c)

  file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/cmake_third_party/crc32c/CMakeLists.txt DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/ext/crc32c)
endif()

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ext/crc32c)