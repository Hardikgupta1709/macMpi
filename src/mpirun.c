#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <poll.h>
#include <sys/wait.h>
#include <signal.h> // For signal handling
#include <string.h>

// Global variables so that the singal handler can access
pid_t *child_pids = NULL;
int num_procs = 0;

// 2. signal handler function

void handle_sigint(int sig)
{
    printf("\n[mpirun] Caught Ctrl+C (SIGINT)! cleaning up clones...\n");

    if (child_pids != NULL)
    {
        for (int i = 0; i < num_procs; i++)
        {
            if (child_pids[i] > 0)
            {
                // send SIGTERM (terminate) to each chid
                printf("[mpirun] Killing clone %d (Pid: %d)\n", i, child_pids[i]);
                kill(child_pids[i], SIGTERM);
            }
        }
    }

    // brief moment for children to die properly to avoid zombies
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

    printf("[mpirun] Cleanup complete. Exiting.\n");
    exit(1);
}

int main(int argc, char *argv[])
{
    if (argc < 4 || strcmp(argv[1], "-n") != 0)
    {
        fprintf(stderr, "Usage: %s -n <num_procs> <executable>\n", argv[0]);
        exit(1);
    }

    num_procs = atoi(argv[2]);
    if (num_procs <= 0)
    {
        fprintf(stderr, "Number of processes must be greater than 0.\n");
        exit(1);
    }

    char *target_executable = argv[3];

    child_pids = malloc(num_procs * sizeof(pid_t));

    struct sigaction sa;
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGINT, &sa, NULL) == -1)
    {
        perror("sigaction");
        exit(1);
    }

    int pipes[num_procs][2];
    struct pollfd pollfds[num_procs];
    int active_pipes = num_procs;

    for (int i = 0; i < num_procs; i++)
    {
        if (pipe(pipes[i]) == -1)
        {
            perror("pipe");
            exit(1);
        }

        pid_t pid = fork();

        if (pid == 0)
        {
            // child process
            close(pipes[i][0]);

            // redirecting stdout and stderr to the pipe

            dup2(pipes[i][1], STDOUT_FILENO);
            dup2(pipes[i][1], STDERR_FILENO);
            close(pipes[i][1]);

            // Injecting the rank environment variable
            char rank_str[10];
            sprintf(rank_str, "%d", i);
            setenv("MPI_RANK", rank_str, 1);

            execlp(target_executable, target_executable, NULL);
            perror("exec failed");

            exit(1);
        }
        else
        {
            // Parent process
            child_pids[i] = pid;
            close(pipes[i][1]);

            // registered the read-end with my pollfd array
            pollfds[i].fd = pipes[i][0];
            pollfds[i].events = POLLIN;
        }
    }

    printf("mpirun: All processses spawned. Listening for output...\n");
    char buffer[1024];

    // polling till we have acitve pipes open
    while (active_pipes > 0)
    {
        int ready = poll(pollfds, num_procs, -1);
        if (ready == -1)
        {
            perror("poll");
            break;
        }

        // looping through all the pipes
        for (int i = 0; i < num_procs; i++)
        {
            // if events show that data is ready to read
            if (pollfds[i].revents & POLLIN)
            {
                ssize_t bytes = read(pollfds[i].fd, buffer, sizeof(buffer) - 1);
                if (bytes > 0)
                {
                    buffer[bytes] = '\0';
                    printf("[Rank %d]: %s", i, buffer);
                }
                else if (bytes == 0)
                {
                    close(pollfds[i].fd);
                    pollfds[i].fd = -1;
                    active_pipes--;
                }
            }
        }
    }

    // waiting for all zombies to finish
    for (int i = 0; i < num_procs; i++)
    {
        wait(NULL);
    }

    free(child_pids);
    return 0;
}
