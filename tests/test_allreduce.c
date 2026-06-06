#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int my_val = rank;
    int total_sum = 0;

    printf("[Rank %d] Entering MPI_Allreduce with valye: %d\n", rank, my_val);

    MPI_Allreduce(&my_val, &total_sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    printf("[Rank %d] MPI_Allreduce COmpleted! Final Synchronised Sum : %d\n", rank, total_sum);

    MPI_Finalize();
    return 0;
}