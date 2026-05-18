#include <stdio.h>
#include <unistd.h>
#include "mpi.h"

int main(int argc, char **argv)
{
    printf("[Main] Calling MPI_Init...\n");
    MPI_Init(&argc, &argv);

    printf("[Main] MPI Initialised. Main thread simulating compute work...\n");

    sleep(2);

    printf("[Main] Work done. Calling MPI_Fianlize...\n");
    MPI_Finalize();

    printf("[Main] MPI Finalized cleanly. Exiting.\n");
    return 0;
}