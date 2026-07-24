

/*
 * monte_carlo_pi.c
 *
 * Phase 3B exit test for mpi-lite.
 *
 * Each rank generates POINTS_PER_RANK random points inside a 2x2 square
 * centered at the origin and counts how many fall inside the unit circle.
 * MPI_Reduce with MPI_SUM combines the per-rank counts on rank 0, which
 * then computes the final pi estimate.
 *
 * This is a genuine integration test: it exercises MPI_Init, MPI_Comm_rank,
 * MPI_Comm_size, MPI_Reduce, and MPI_Finalize all in one program, and the
 * correctness of the result (does it converge to ~3.14159) is an actual
 * mathematical proof that your reduction logic is not silently corrupting
 * data — a wrong MPI_Reduce will not crash, it will just give you a wrong
 * pi value that looks plausible at a glance, so check the decimal places.
 *
 * Build (link against your own mpi-lite library):
 *   clang -I/path/to/mpi-lite/include monte_carlo_pi.c \
 *         -L/path/to/mpi-lite/lib -lmpi-lite -lm \
 *         -o monte_carlo_pi
 *
 * Run:
 *   ./mpirun -n 8 ./monte_carlo_pi
 *
 * Expected output: a single line from rank 0 with a pi estimate accurate
 * to at least 2-3 decimal places when run with 8 ranks and the point
 * count below. More points and more ranks tightens the estimate further,
 * but this is bounded by Monte Carlo's sqrt(N) convergence, not by your
 * MPI implementation, so do not expect perfect precision.
 */

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

#define POINTS_PER_RANK 10000000UL /* 10 million points per rank */

int main(int argc, char **argv)
{
    int rank, size;
    int ret;

    ret = MPI_Init(&argc, &argv);
    if (ret != MPI_SUCCESS)
    {
        fprintf(stderr, "MPI_Init failed with code %d\n", ret);
        return 1;
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* Seed each rank's RNG differently. Using rank + time ensures ranks
     * launched in the same second still get distinct streams, which
     * matters because all ranks share the same physical machine and
     * could otherwise seed identically and produce correlated points. */
    unsigned int seed = (unsigned int)(time(NULL)) ^ (unsigned int)(rank * 7919 + 104729);
    srand(seed);

    long local_inside = 0;

    for (unsigned long i = 0; i < POINTS_PER_RANK; i++)
    {
        double x = ((double)rand() / (double)RAND_MAX) * 2.0 - 1.0; /* [-1, 1] */
        double y = ((double)rand() / (double)RAND_MAX) * 2.0 - 1.0; /* [-1, 1] */

        if (x * x + y * y <= 1.0)
        {
            local_inside++;
        }
    }

    printf("[Rank %d/%d] local points inside circle: %ld / %lu\n",
           rank, size, local_inside, POINTS_PER_RANK);
    fflush(stdout);

    long global_inside = 0;

    /* This single MPI_Reduce call is the actual test. If your reduction
     * tree, your MPI_SUM operation, or your message envelope handling
     * has any bug, global_inside will not equal the true sum of every
     * rank's local_inside, and the pi estimate below will be visibly
     * wrong (not just imprecise — actually wrong, e.g. 2.1 or 4.8). */
    ret = MPI_Reduce(&local_inside, &global_inside, 1, MPI_INT,
                     MPI_SUM, 0, MPI_COMM_WORLD);
    if (ret != MPI_SUCCESS)
    {
        fprintf(stderr, "[Rank %d] MPI_Reduce failed with code %d\n", rank, ret);
        MPI_Finalize();
        return 1;
    }

    if (rank == 0)
    {
        unsigned long total_points = (unsigned long)size * POINTS_PER_RANK;
        double pi_estimate = 4.0 * (double)global_inside / (double)total_points;
        double error = fabs(pi_estimate - M_PI);

        printf("\n===== Monte Carlo Pi Estimation Result =====\n");
        printf("Processes used:        %d\n", size);
        printf("Total points sampled:  %lu\n", total_points);
        printf("Points inside circle:  %ld\n", global_inside);
        printf("Pi estimate:           %.6f\n", pi_estimate);
        printf("Actual pi (M_PI):      %.6f\n", M_PI);
        printf("Absolute error:        %.6f\n", error);
        printf("=============================================\n\n");

        if (error < 0.01)
        {
            printf("PASS: estimate within 0.01 of actual pi.\n");
        }
        else if (error < 0.05)
        {
            printf("MARGINAL: estimate within 0.05 but worse than expected for %lu points.\n",
                   total_points);
        }
        else
        {
            printf("FAIL: estimate is off by more than 0.05 — check your "
                   "MPI_Reduce implementation, this usually means data is "
                   "being dropped, double-counted, or corrupted in the "
                   "reduction tree.\n");
        }
    }

    MPI_Finalize();
    return 0;
}