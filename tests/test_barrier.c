#include "mpi.h"
#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int sleep_time = rank * 2;
    printf("[Rank %d] doiing %d seconds of heavy math...\n", rank, sleep_time);
    sleep(sleep_time);

    printf("[Rank %d] Reached the barrrier. Waiting for others...\n", rank);

    MPI_Barrier(MPI_COMM_WORLD);

    printf("[Rank %d] Survived the barrier! Continuing execution...\n", rank);

    MPI_Finalize();
    return 0;
}