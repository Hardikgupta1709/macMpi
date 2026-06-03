#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Override C library pipe buffering so mpirun sees output instantly
    setvbuf(stdout, NULL, _IONBF, 0);

    int elements_per_proc = 2;
    int *send_data = NULL;
    int *recv_data = (int *)malloc(elements_per_proc * sizeof(int));

    // Root initializes the grand matrix
    if (rank == 0)
    {
        send_data = (int *)malloc(size * elements_per_proc * sizeof(int));
        printf("[Rank 0] Preparing data to scatter: ");
        for (int i = 0; i < size * elements_per_proc; i++)
        {
            send_data[i] = (i + 1) * 10;
            printf("%d ", send_data[i]); // Added a space here so numbers don't mush together
        }
        printf("\n");
    }

    // Synchronize terminal output
    MPI_Barrier(MPI_COMM_WORLD);

    // Perform the Scatter
    MPI_Scatter(send_data, elements_per_proc, MPI_INT,
                recv_data, elements_per_proc, MPI_INT,
                0, MPI_COMM_WORLD);

    // Verify (Fixed format string)
    printf("[Rank %d] Received: %d, %d\n", rank, recv_data[0], recv_data[1]);

    if (rank == 0)
    {
        free(send_data);
    }
    free(recv_data);

    MPI_Finalize();
    return 0;
}