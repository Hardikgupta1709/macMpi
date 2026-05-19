#include <stdio.h>
#include <unistd.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    printf("[Main] Calling MPI_Init...\n");
    MPI_Init(&argc, &argv);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0)
    {
        printf("\n--- Rank 0: Testing Non-Blocking Send ---\n");

        int my_data = 42;
        MPI_Request req;

        MPI_Isend(&my_data, 1, MPI_INT, 1, 99, MPI_COMM_WORLD, &req);

        // Main thread is instantly free to do math
        printf("[Main] MPI_Isend returned immediately! I am doing other math now...\n");
        sleep(2);

        printf("[Main] Math done, Shutting down.\n");
    }
    else
    {
        sleep(3);
    }

    MPI_Finalize();

    printf("[Main] MPI Finalized cleanly. Exiting.\n");
    return 0;
}