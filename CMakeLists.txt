cmake_minimum_required(VERSION 3.16)
project(ipc_packets LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")

add_executable(producer src/producer.cpp)
add_executable(consumer src/consumer.cpp)
add_executable(ring_selftest tests/ring_selftest.cpp)
find_package(Threads REQUIRED)
target_link_libraries(ring_selftest PRIVATE Threads::Threads)

foreach(tgt producer consumer ring_selftest)
    target_include_directories(${tgt} PRIVATE include)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_link_libraries(${tgt} PRIVATE rt)
    endif()
endforeach()
