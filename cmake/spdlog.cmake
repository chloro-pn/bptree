include(FetchContent)

set(SPDLOG_GIT_TAG v1.10.0)
set(SPDLOG_GIT_URL https://ghproxy.com/https://github.com/gabime/spdlog)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ext/spdlog)
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY ${SPDLOG_GIT_URL}
    GIT_TAG ${SPDLOG_GIT_TAG}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext/spdlog
  )

  FetchContent_MakeAvailable(spdlog)
else()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ext/spdlog)
endif()