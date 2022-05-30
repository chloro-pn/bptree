include(FetchContent)

set(GT_GIT_TAG release-1.11.0)
set(GT_GIT_URL git@github.com:google/googletest.git)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ext/googletest)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY ${GT_GIT_URL}
    GIT_TAG ${GT_GIT_TAG}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext/googletest
  )

  FetchContent_MakeAvailable(googletest)
else()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ext/googletest)
endif()