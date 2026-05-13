cmake_minimum_required(VERSION 3.20)
project(mpi_lite C)

# Set C standard to C99 or C11
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Add include directory so compiler finds mpi.h later
include_directories(include)

# Tell CMake to output compile commands for the clangd extension
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Create a dummy executable for now
add_executable(mpirun src/mpirun.c)

# to compile test program alongside mpi run
add_executable(hello_env tests/hello_env.c)