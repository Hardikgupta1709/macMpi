#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char *argv[])
{

    // Reading the injected variables
    char *rank_env = getenv("MPI_RANK");
    char *size_env = getenv("MPI_UNIVERSE_SIZE");

    if (rank_env == NULL || size_env == NULL)
    {
        fprintf(stderr, "Error: Must be run via mpirun!\n");
        return 1;
    }

    int rank = atoi(rank_env);
    int size = atoi(size_env);

    printf("Hello from Rank %d out of %d! (My Pid is %d)\n", rank, size, getpid());

    return 0;
}