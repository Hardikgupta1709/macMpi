cmake_minimum_required(VERSION 3.20)
project(macMPI VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(mpi SHARED
    src/mpi.c
    src/engine.c
    src/collectives.c
)

target_include_directories(mpi PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)


# Process Manager (mpirun)
add_executable(mpirun src/mpirun.c)
target_link_libraries(mpirun PRIVATE mpi)


add_executable(hello_env tests/hello_env.c)
target_link_libraries(hello_env PRIVATE mpi)

add_executable(test_engine tests/test_engine.c)
target_link_libraries(test_engine PRIVATE mpi)

add_executable(test_kqueue tests/test_kqueue.c)
target_link_libraries(test_kqueue PRIVATE mpi)

add_executable(test_barrier tests/test_barrier.c)
target_link_libraries(test_barrier PRIVATE mpi)

add_executable(test_bcast tests/test_bcast.c)
target_link_libraries(test_bcast PRIVATE mpi)

add_executable(test_reduce tests/test_reduce.c)
target_link_libraries(test_reduce PRIVATE mpi)

add_executable(test_scatter tests/test_scatter.c)
target_link_libraries(test_scatter PRIVATE mpi)

add_executable(test_gather tests/test_gather.c)
target_link_libraries(test_gather PRIVATE mpi)

add_executable(test_allgather tests/test_allgather.c)
target_link_libraries(test_allgather PRIVATE mpi)

add_executable(test_allreduce tests/test_allreduce.c)
target_link_libraries(test_allreduce PRIVATE mpi)

add_executable(test_alltoall tests/test_alltoall.c)
target_link_libraries(test_alltoall PRIVATE mpi)


# telling macOS exactly where to put the binary files a user runs 'sudo make install'
install(TARGETS mpi mpirun
    LIBRARY DESTINATION lib     # libmpi.dylib goes to /user/local/lib
    RUNTIME DESTINATION bin     # mpirun executable goes to user/local/bin
)

# telling macOS where to put the pubic header file 

install(FILES include/mpi.h
    DESTINATION include     # mpi.h goes to /user/local/include
)