cmake_minimum_required(VERSION 3.16)
project(kubera_executor_cpp LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(kubera_executor_cpp STATIC
  src/kubera_executor.cpp
)

target_include_directories(kubera_executor_cpp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Install target required by Rust cmake crate
install(TARGETS kubera_executor_cpp ARCHIVE DESTINATION lib)
install(FILES src/kubera_executor.h DESTINATION include)
