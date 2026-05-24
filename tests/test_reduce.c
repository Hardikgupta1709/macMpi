#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Test data
    int send_val = rank;
    int recv_val = -1;

    int root = 0;

    printf("[Rank %d] Entering MPI_Reduce with value : %d\n", rank, send_val);

    MPI_Reduce(&send_val, &recv_val, 1, MPI_INT, MPI_SUM, root, MPI_COMM_WORLD);

    if (root == rank)
    {
        int expected_sum = (size - 1) * size / 2;

        printf("[Root %d] MPI_reduce Completed!\n", root);
        printf("[Root %d] Calculated Sum: %d\n", root, recv_val);
        printf("[Root %d] Expected Sum: %d\n", root, expected_sum);

        if (recv_val == expected_sum)
        {
            printf(">>> SUCCESS \n");
        }
        else
        {
            printf(">>> FAILURE: Data corruption or topology routing error. \n");
        }
    }
    else
    {
        printf("[Rank %d] Finished reduction, passed data up the tree.\n", rank);
    }
    MPI_Finalize();
    return 0;
}