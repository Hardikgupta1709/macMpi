#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>    // getopt, fork, getpid
#include <sys/wait.h>  // waitpid
#include <sys/types.h> // pid_t

// 1. State definitions
typedef struct
{
    int rank;
    pid_t pid;
    int exit_status;
    int is_alive;
} RankContext;

typedef struct
{
    int num_ranks;      // Total processes
    RankContext *ranks; // Array of children
} MpiRunState;

// 2. Main Process Manager
int main(int argc, char *argv[])
{
    int opt;
    MpiRunState state;
    state.num_ranks = 1;

    // A. Parse command line arguments
    while ((opt = getopt(argc, argv, "n:")) != -1)
    {
        if (opt == 'n')
        {
            state.num_ranks = atoi(optarg);
        }
    }

    // B. Get the user's executable (everything after -n)
    // 'optind' is the index of the first non-flag argument

    if (optind >= argc)
    {
        fprintf(stderr, "Usage: %s -n <num_procs> <executable> [args...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *target_executable = argv[optind];
    char **target_args = &argv[optind]; // Parses the program and any of its arguments

    printf("[mpirun] Launching %d instances of '%s'...\n", state.num_ranks, target_executable);

    // B. Allocate memory
    state.ranks = malloc(state.num_ranks * sizeof(RankContext));

    // C. Fork loop
    for (int i = 0; i < state.num_ranks; i++)
    {
        state.ranks[i].rank = i;
        state.ranks[i].is_alive = 1;

        pid_t pid = fork();

        if (pid < 0)
        {
            perror("fork failed");
            exit(EXIT_FAILURE);
        }
        else if (pid == 0)
        {
            // 1. integers to strings for the environment
            char rank_str[16], size_str[16];
            snprintf(rank_str, sizeof(rank_str), "%d", i);
            snprintf(size_str, sizeof(size_str), "%d", state.num_ranks);

            // 2. Inject Environment Variables
            setenv("MPI_RANK", rank_str, 1);
            setenv("MPI_UNIVERSE_SIZE", size_str, 1);

            // 3. The exec() Boundary
            execvp(target_executable, target_args);

            // 4. If we reach this line, execvp failed
            fprintf(stderr, "[Rank %d] failed to execute %s\n", i, target_executable);
            perror("execvp error");
            exit(EXIT_FAILURE);
        }
        else
        {
            // Parent process
            state.ranks[i].pid = pid;
        }
    }

    // D. Wait loop
    int active_processes = state.num_ranks;

    while (active_processes > 0)
    {
        int status;
        pid_t dead_pid = waitpid(-1, &status, 0);

        if (dead_pid > 0)
        {
            for (int i = 0; i < state.num_ranks; i++)
            {
                if (state.ranks[i].pid == dead_pid)
                {
                    state.ranks[i].is_alive = 0;

                    if (WIFEXITED(status))
                    {
                        state.ranks[i].exit_status = WEXITSTATUS(status);
                        printf("[mpirun] Rank %d exited gracefully with code %d\n", i, state.ranks[i].exit_status);
                    }
                    else
                    {
                        printf("[mpirun] Rank %d terminated abnormally!\n", i);
                    }

                    active_processes--;
                    break;
                }
            }
        }
    }

    printf("[mpirun] All ranks have terminated. Job complete.\n");

    free(state.ranks);
    return 0;
}