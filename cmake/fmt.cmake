include(FetchContent)

set(GT_GIT_TAG 8.1.1)
set(GT_GIT_URL https://ghproxy.com/https://github.com/fmtlib/fmt)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ext/fmt)
  FetchContent_Declare(
    fmt
    GIT_REPOSITORY ${GT_GIT_URL}
    GIT_TAG ${GT_GIT_TAG}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext/fmt
  )

  FetchContent_MakeAvailable(fmt)
else()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ext/fmt)
endif()