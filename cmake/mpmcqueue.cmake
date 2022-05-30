include(FetchContent)

set(QUEUE_GIT_URL git@github.com:rigtorp/MPMCQueue.git)

if (NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ext/mpmcqueue)
  FetchContent_Declare(
    mpmcqueue
    GIT_REPOSITORY ${QUEUE_GIT_URL}
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext/mpmcqueue
  )

  FetchContent_MakeAvailable(mpmcqueue)

  #file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/cmake_third_party/mpmcqueue/CMakeLists.txt DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/ext/mpmcqueue)
else()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ext/mpmcqueue)
endif()