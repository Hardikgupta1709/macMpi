#include "../include/mpi.h"
#include <stdio.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 2)
    {
        if (rank == 0)
        {
            printf("Ping-pong requires at least 2 processes. You provided %d.\n", size);
        }
        MPI_Finalize();
        return 0;
    }

    int ping_pong_count = 0;
    int target_rank = 1;
    MPI_Status status;

    if (rank == 0)
    {
        ping_pong_count = 42;
        printf("[Rank 0] Sending payload (%d) to Rank 1...\n", ping_pong_count);

        // 1. Send to rank 1
        MPI_Send(&ping_pong_count, 1, MPI_INT, target_rank, 0, MPI_COMM_WORLD);

        MPI_Recv(&ping_pong_count, 1, MPI_INT, target_rank, 0, MPI_COMM_WORLD, &status);

        printf("[Rank 0] Recieved returning payload (%d) from Rank 1! \n", ping_pong_count);
    }
    else if (rank == 1)
    {
        MPI_Recv(&ping_pong_count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        printf("[Rank 1] Recieved payload (%d) from Rank 0.\n", ping_pong_count);

        ping_pong_count = ping_pong_count * 2;

        printf("[Rank 1] Modified payload to %d. Sending back to Rank 0..\n", ping_pong_count);
        MPI_Send(&ping_pong_count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}