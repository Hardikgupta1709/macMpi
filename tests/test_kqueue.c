#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 3)
    {
        if (rank == 0)
        {
            printf("Please run atleast 3 processess.\n");
            MPI_Finalize();
            return 0;
        }
    }
    if (rank == 0)
    {
        printf("\n--- [Rank 0] Kqueue stree Test & MPI_Test Validation ---\n ");

        int *buffers = malloc(size * sizeof(int));
        MPI_Request *reqs = malloc(size * sizeof(MPI_Request));

        // Post receives for every other rank simultaenously
        for (int i = 1; i < size; i++)
        {
            buffers[i] = 0;
            MPI_Irecv(&buffers[i], 1, MPI_INT, i, 100, MPI_COMM_WORLD, &reqs[i]);
        }

        int completed_count = 1; // from 1 As rank 0 doesn't receive from itself
        int *is_done = calloc(size, sizeof(int));

        // Instead of halting on MPI_Wait, using MPI_Test so that rank 0 keeps spinning, allowing it to do compute
        while (completed_count < size)
        {
            for (int i = 1; i < size; i++)
            {
                if (!is_done[i])
                {
                    int flag = 0;

                    // MPI_Test checks instantly kqueue's progress and returns control
                    MPI_Test(&reqs[i], &flag, MPI_STATUS_IGNORE);

                    if (flag)
                    {
                        printf("[Rank 0] Background thread successfully delivered data form Rank %d: %d\n", i, buffers[i]);
                        fflush(stdout);
                        is_done[i] = 1;
                        completed_count++;
                    }
                }
            }
            // simulating other useful computations while the network transfers
            usleep(50000);
        }

        printf("[Rank 0] Kqueue perfectly batched and delivered all messages\n");

        free(buffers);
        free(reqs);
        free(is_done);
    }
    else
    {
        // All other ranks blast a message to rank 0 at the exact same time
        int my_data = rank * 1000;
        MPI_Request req;

        // slight sleep to ensure Rank 0 has posted all its receives first
        usleep(10000);

        printf("[Rank %d] Blasting data (%d) to Rank 0...\n.", rank, my_data);
        MPI_Isend(&my_data, 1, MPI_INT, 0, 100, MPI_COMM_WORLD, &req);

        MPI_Wait(&req, MPI_STATUS_IGNORE);
    }

    fflush(stdout);
    sleep(1);

    MPI_Finalize();
    return 0;
}