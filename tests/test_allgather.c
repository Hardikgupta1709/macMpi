#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Each process contributes exactly 2 elements

    int elements_per_proc = 2;
    int send_data[2];

    send_data[0] = (rank * 10) + 1;
    send_data[1] = (rank * 10) + 2;

    // Allocating enough space to receive 2 elements from ALL processes
    int total_elements = elements_per_proc * size;
    int *recv_data = (int *)malloc(total_elements * sizeof(int));

    // synchronising terminal before the collective
    MPI_Barrier(MPI_COMM_WORLD);
    printf("[Rank %d] Ready to allgather data...\n", rank);

    MPI_Allgather(send_data, elements_per_proc, MPI_INT, recv_data, elements_per_proc, MPI_INT, MPI_COMM_WORLD);

    printf("[Rank %d] Final Array : [ ", rank);

    for (int i = 0; i < total_elements; i++)
    {
        printf("%d", recv_data[i]);
    }

    printf("]\n");

    free(recv_data);
    MPI_Finalize();
    return 0;
}
