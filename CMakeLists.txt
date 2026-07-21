cmake_minimum_required(VERSION 3.25)

project(
        ipc_packets
        VERSION 1.0.0
        LANGUAGES CXX
)

find_package(CLI11 REQUIRED)
find_package(Crc32c REQUIRED)
find_package(spdlog REQUIRED)
find_package(Threads REQUIRED)

add_executable(producer
        src/source/producer/main.cpp
        src/source/producer/producer.cpp
)
target_include_directories(producer PRIVATE ${PROJECT_SOURCE_DIR}/src/include/producer)

add_executable(consumer
        src/source/consumer/main.cpp
        src/source/consumer/consumer.cpp
)
target_include_directories(consumer PRIVATE ${PROJECT_SOURCE_DIR}/src/include/consumer)

function(setup_target target)
    target_compile_features(${target} PRIVATE cxx_std_20)

    target_include_directories(${target}
            PRIVATE
            ${PROJECT_SOURCE_DIR}/src/include/common
    )

    target_compile_options(${target}
            PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            $<$<CONFIG:Release>:-O3>
    )

    target_link_libraries(${target}
            PRIVATE
            CLI11::CLI11
            Crc32c::crc32c
            spdlog::spdlog
            Threads::Threads
    )
endfunction()

setup_target(producer)
setup_target(consumer)