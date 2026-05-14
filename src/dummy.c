#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main()
{
    // Injected environment variable
    char *rank_env = getenv("MPI_RANK");
    int rank = -1;

    if (rank_env != NULL)
    {
        rank = atoi(rank_env);
    }

    printf("Hello! I'm alive. My MPI_RANK is %d. \n", rank);

    // flushing stdout so that it goes into the pipe
    fflush(stdout);

    for (int i = 0; i <= 3; i++)
    {
        sleep(2);
        printf("Working Hard...(Step %d/3) form Rank %d\n", i, rank);
        fflush(stdout);
    }

    printf("Rank %d finished successfully! \n", rank);
    return 0;
}