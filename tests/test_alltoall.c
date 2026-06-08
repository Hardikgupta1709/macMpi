#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Allocating the buffer, exactly (size * sendcount)
    // sending 1 process to every other process exactly
    int *sendbuf = malloc(size * sizeof(int));
    int *recvbuf = malloc(size * sizeof(int));

    for (int i = 0; i < size; i++)
    {
        sendbuf[i] = (rank * 10) + i;
        recvbuf[i] = -1;
    }

    if (rank == 0)
    {
        printf("[Rank 0 ] Initialising Distributes matrix transpose...\n");
    }

    usleep(10000);

    MPI_Alltoall(sendbuf, 1, MPI_INT, recvbuf, 1, MPI_INT, MPI_COMM_WORLD);

    char output[256] = "";
    char temp[16];

    for (int i = 0; i < size; i++)
    {
        sprintf(temp, "%02d", recvbuf[i]);
        strcat(output, temp);
    }

    printf("[Rank %d] Final Recevied matrix Row: [%s]\n", rank, output);

    free(sendbuf);
    free(recvbuf);

    MPI_Finalize();
    return 0;
}
