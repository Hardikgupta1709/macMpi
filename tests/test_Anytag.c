#include "../include/mpi.h"
#include <stdio.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Status status;
    int buff = rank;

    if (rank == 0)
    {
        printf("[Rank 0] Starting the chain, sending data to Rank 1...\n");
        MPI_Send(&buff, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
    }
    else if (rank < size - 1)
    {
        MPI_Recv(&buff, 1, MPI_INT, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        printf("[Rank %d] Received data from Rank %d (Tag: %d). Passing it along...\n", rank, status.MPI_SOURCE, status.MPI_TAG);

        MPI_Send(&buff, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
    }
    else if (rank == size - 1)
    {
        MPI_Recv(&buff, 1, MPI_INT, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        printf("[Rank %d] End of the line! Received data from Rank %d (Tag: %d).\n", rank, status.MPI_SOURCE, status.MPI_TAG);
    }

    MPI_Finalize();
    return 0;
}