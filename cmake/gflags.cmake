include(FetchContent)

set(GT_GIT_TAG v2.2.2)
set(GT_GIT_URL https://ghproxy.com/https://github.com/gflags/gflags)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ext/gflags)
  FetchContent_Declare(
    gflags
    GIT_REPOSITORY ${GT_GIT_URL}
    GIT_TAG ${GT_GIT_TAG}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext/gflags
  )

  FetchContent_MakeAvailable(gflags)
else()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ext/gflags)
endif()