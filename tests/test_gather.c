#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int root = 0;
    int element_per_proc = 2;

    int send_data[2];
    send_data[0] = (rank * 10) + 1;
    send_data[1] = (rank * 10) + 2;

    int *recv_data = NULL;
    if (rank == root)
    {
        recv_data = (int *)malloc(size * element_per_proc * sizeof(int));
        printf("[Root %d] Ready to gather data from %d processes...\n", root, size);
    }

    MPI_Gather(send_data, element_per_proc, MPI_INT, recv_data, element_per_proc, MPI_INT, root, MPI_COMM_WORLD);

    // Root prints the overall result
    if (rank == root)
    {
        printf("[Root %d] Gather Complete. Final Array: [ ", root);
        for (int i = 0; i < size * element_per_proc; i++)
        {
            printf("%d", recv_data[i]);
        }
        printf("]\n");
        free(recv_data);
    }

    MPI_Finalize();
    return MPI_SUCCESS;
}
