cmake_minimum_required(VERSION 3.10)
project(QueryProcessor)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Tell CMake where to find our header files (for nlohmann::json)
include_directories(include)

# Tell CMake to build an executable named 'query_processor' from our main file
add_executable(query_processor src/main.cpp)