
cmake_minimum_required(VERSION 3.3)

project(RWLock CXX ASM)

# set(CMAKE_CXX_STANDARD 11)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3 -g")

include_directories(
    core
    thirdparty
    workload
    compute_pool
    test
)

add_subdirectory(thirdparty/rlib)
add_subdirectory(thirdparty/fcontext)
add_subdirectory(thirdparty/smart)
add_subdirectory(test)
add_subdirectory(core)

add_subdirectory(workload)

add_subdirectory(compute_pool)

add_subdirectory(memory_pool)