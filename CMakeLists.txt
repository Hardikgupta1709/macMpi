cmake_minimum_required(VERSION 3.20)
project(mpi_lite C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

include_directories(include)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# MPI library
add_library(mpi
    src/mpi.c
    src/engine.c
    src/collectives.c
)

# mpirun executable
add_executable(mpirun
    src/mpirun.c
    src/collectives.c
)
target_link_libraries(mpirun mpi)

# test_engine executable
add_executable(test_engine
    tests/test_engine.c
)
target_link_libraries(test_engine mpi)

# hello test
add_executable(hello_env
    tests/hello_env.c
)
target_link_libraries(hello_env mpi)

# kqueue test
add_executable(test_kqueue
    tests/test_kqueue.c
)
target_link_libraries(test_kqueue mpi)

add_executable(test_barrier
    tests/test_barrier.c   
)
target_link_libraries(test_barrier mpi)

add_executable(test_bcast
    tests/test_bcast.c
    src/collectives.c
)
target_link_libraries(test_bcast mpi)

add_executable(test_reduce
    tests/test_reduce.c
)
target_link_libraries(test_reduce mpi)

add_executable(test_scatter
    tests/test_scatter.c
)
target_link_libraries(test_scatter mpi)

add_executable(test_gather
    tests/test_gather.c    
)
target_link_libraries(test_gather mpi)

add_executable(test_allgather
    tests/test_allgather.c
)
target_link_libraries(test_allgather mpi)

add_executable(test_allreduce
    tests/test_allreduce.c
)
target_link_libraries(test_allreduce mpi)

add_executable(test_alltoall
    tests/test_alltoall.c
)
target_link_libraries(test_alltoall mpi)