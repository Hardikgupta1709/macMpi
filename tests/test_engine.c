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
        printf("\n--- Rank 0: Testing Full Async Lifecycle ---\n");

        int my_data = 999;
        MPI_Request req;

        MPI_Isend(&my_data, 1, MPI_INT, 1, 42, MPI_COMM_WORLD, &req);

        printf("[Rank 0] MPI_Isend returned immediately! Simulating heavy math...\n");
        sleep(2);

        printf("[Rank 0] Math done. Calling MPI_Wait...\n");
        MPI_Wait(&req, MPI_STATUS_IGNORE);
        printf("[Rank 0] MPI_Wait returned! Background transfer is 100%% complete.\n");
    }
    else if (rank == 1)
    {
        int received_data = 0;

        MPI_Recv(&received_data, 1, MPI_INT, 0, 42, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("[Rank 1] Successfulyly received data: %d\n", received_data);

        fflush(stdout);

        sleep(3);
    }

    MPI_Finalize();

    printf("[Main] MPI Finalized cleanly. Exiting.\n");
    return 0;
}