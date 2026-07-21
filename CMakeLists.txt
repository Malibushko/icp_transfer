cmake_minimum_required(VERSION 3.25)

project(
        ipc_packets
        VERSION 1.0.0
        LANGUAGES CXX
)

find_package(Crc32c REQUIRED)
find_package(jnk0le_ringbuffer REQUIRED)
find_package(Threads REQUIRED)

add_executable(producer
        src/producer.cpp
)

add_executable(consumer
        src/consumer.cpp
)

function(setup_target target)
    target_compile_features(${target} PRIVATE cxx_std_20)

    target_include_directories(${target}
            PRIVATE
            ${PROJECT_SOURCE_DIR}/include
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
            Crc32c::crc32c
            Threads::Threads
    )
endfunction()

setup_target(producer)
setup_target(consumer)