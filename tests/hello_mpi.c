#include "../include/mpi.h"
#include <stdio.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int num_rank;
    int num_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &num_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_size);

    printf("Hello form actual MPI! I'm rank %d out of %d processes.\n", num_rank, num_size);

    MPI_Finalize();

    return 0;
}