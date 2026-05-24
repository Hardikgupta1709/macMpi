#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int size, rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int count = 5;
    int *buffer = (int *)malloc(count * sizeof(int));
    int root = 0;

    if (rank == root)
    {
        // Initialize the buffer woth dummy data on the root
        for (int i = 0; i < count; i++)
        {
            buffer[i] = i * 10;
        }

        printf("[Rank %d] Root broadcasting data: ", rank);
        fflush(stdout);
        for (int i = 0; i < count; i++)
        {
            printf("%d", buffer[i]);
        }
        printf("\n");
        fflush(stdout);
    }
    else
    {
        // Initialize other processes with garbage to prove the broadcast works
        for (int i = 0; i < count; i++)
        {
            buffer[i] = -1;
        }
    }

    MPI_Bcast(buffer, count, MPI_INT, root, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);

    // verify the data was received
    printf("[Rank %d] Recieved data: ", rank);
    for (int i = 0; i < count; i++)
    {
        printf("%d", buffer[i]);
    }
    printf("\n");
    fflush(stdout);

    for (int i = 0; i < count; i++)
    {
        if (buffer[i] != i * 10)
        {
            printf("[Rank %d] FAILED at index %d! Expected %d, got %d\n", rank, i, i * 10, buffer[i]);
            fflush(stdout);
            break;
        }
    }

    free(buffer);
    MPI_Finalize();
    return 0;
}
