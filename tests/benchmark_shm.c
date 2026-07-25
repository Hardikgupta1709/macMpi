#include "../include/mpi.h"
#include <stdio.h>
#include <stdlib.h>

#define ITERATIONS 1000
#define ARRAY_SIZE 2500000

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
            printf("Benchmark requires at least 2 processes.\n");
            MPI_Finalize();
            return 0;
        }
    }

    size_t payload_bytes = ARRAY_SIZE * sizeof(double);
    double *buffer = (double *)malloc(payload_bytes);

    for (int i = 0; i < ARRAY_SIZE; i++)
    {
        buffer[i] = (double)rank;
    }

    if (rank == 0)
    {
        for (int i = 0; i < 10; i++)
        {
            MPI_Send(buffer, ARRAY_SIZE, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD);
            MPI_Recv(buffer, ARRAY_SIZE, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }
    else if (rank == 1)
    {
        for (int i = 0; i < 10; i++)
        {
            MPI_Recv(buffer, ARRAY_SIZE, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(buffer, ARRAY_SIZE, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }
    }

    // synchronising both ranks
    MPI_Barrier(MPI_COMM_WORLD);

    double start_time = MPI_Wtime();

    if (rank == 0)
    {
        for (int i = 0; i < ITERATIONS; i++)
        {
            MPI_Send(buffer, ARRAY_SIZE, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD);
            MPI_Recv(buffer, ARRAY_SIZE, MPI_DOUBLE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }
    else if (rank == 1)
    {
        for (int i = 0; i < ITERATIONS; i++)
        {
            MPI_Recv(buffer, ARRAY_SIZE, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(buffer, ARRAY_SIZE, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }
    }

    double end_time = MPI_Wtime();

    if (rank == 0)
    {
        double total_time = end_time - start_time;

        double total_bytes = (double)payload_bytes * 2 * ITERATIONS;
        double gigabytes = total_bytes / (1024.0 * 1024.0 * 1024.0);

        double bandwidth = gigabytes / total_time;

        printf("\n==========================================\n");
        printf("   macMPI Shared Memory Benchmark\n");
        printf("==========================================\n");
        printf("Payload Size : %.2f MB\n", payload_bytes / (1024.0 * 1024.0));
        printf("Iterations   : %d\n", ITERATIONS);
        printf("Total Time   : %.4f seconds\n", total_time);
        printf("------------------------------------------\n");
        printf("Bandwidth    : %.2f GB/s\n", bandwidth);
        printf("==========================================\n\n");
    }

    free(buffer);
    MPI_Finalize();
    return 0;
}