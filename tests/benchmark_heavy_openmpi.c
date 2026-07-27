// tests/benchmark_heavy.c
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// 2048 x 2048 Matrix = 33.5 Megabytes per matrix
#define N 2048

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (N % size != 0)
    {
        if (rank == 0)
            printf("Error: Matrix size N (%d) must be divisible by number of processes (%d).\n", N, size);
        MPI_Finalize();
        return 1;
    }

    int rows_per_process = N / size;

    // Allocate memory on the heap (These are too big for the stack)
    double *A = NULL, *B = NULL, *C = NULL;
    double *local_A = (double *)malloc(rows_per_process * N * sizeof(double));
    double *local_C = (double *)malloc(rows_per_process * N * sizeof(double));
    B = (double *)malloc(N * N * sizeof(double));

    if (rank == 0)
    {
        A = (double *)malloc(N * N * sizeof(double));
        C = (double *)malloc(N * N * sizeof(double));
        // Initialize matrices with dummy data
        for (int i = 0; i < N * N; i++)
        {
            A[i] = 1.0;
            B[i] = 2.0;
        }
        printf("[Rank 0] Matrices Allocated. Starting Heavy Compute + Network Benchmark...\n");
    }

    // Synchronize all ranks before starting the clock
    MPI_Barrier(MPI_COMM_WORLD);
    double start_time = MPI_Wtime();

    // 1. MASSIVE NETWORK LOAD: Broadcast the entire 33.5MB Matrix B to all processes
    MPI_Bcast(B, N * N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // 2. MASSIVE NETWORK LOAD: Scatter the 33.5MB Matrix A so every process gets a chunk of rows
    MPI_Scatter(A, rows_per_process * N, MPI_DOUBLE,
                local_A, rows_per_process * N, MPI_DOUBLE,
                0, MPI_COMM_WORLD);

    // 3. MASSIVE COMPUTE LOAD: 17.1 Billion Floating Point Operations
    for (int i = 0; i < rows_per_process; i++)
    {
        for (int j = 0; j < N; j++)
        {
            double sum = 0.0;
            for (int k = 0; k < N; k++)
            {
                sum += local_A[i * N + k] * B[k * N + j];
            }
            local_C[i * N + j] = sum;
        }
    }

    // 4. MASSIVE NETWORK LOAD: Gather the computed chunks back into Matrix C on Rank 0
    MPI_Gather(local_C, rows_per_process * N, MPI_DOUBLE,
               C, rows_per_process * N, MPI_DOUBLE,
               0, MPI_COMM_WORLD);

    double end_time = MPI_Wtime();

    if (rank == 0)
    {
        double total_time = end_time - start_time;
        // 2 * N^3 calculates the exact number of multiply-add operations
        double gflops = (2.0 * N * N * N) / (total_time * 1e9);

        printf("\n==========================================\n");
        printf("   HEAVY HPC BENCHMARK: Matrix Multiplication\n");
        printf("==========================================\n");
        printf("Matrix Size     : %d x %d\n", N, N);
        printf("Total Data Moved: %.2f MB\n", (3.0 * N * N * sizeof(double)) / (1024 * 1024));
        printf("Total Time      : %.4f seconds\n", total_time);
        printf("------------------------------------------\n");
        printf("Compute Power   : %.2f GFLOPS\n", gflops);
        printf("==========================================\n\n");
        free(A);
        free(C);
    }

    free(local_A);
    free(local_C);
    free(B);
    MPI_Finalize();
    return 0;
}