#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <poll.h>
#include <sys/wait.h>
#include <signal.h> // For signal handling
#include <string.h>
#include <sys/socket.h>

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

    int *socket_mesh = calloc(num_procs * num_procs, sizeof(int));
    if (socket_mesh == NULL)
    {
        fprintf(stderr, "[mpirun] Fatal: Out of memory allocating socket mesh.\n");
        exit(EXIT_FAILURE);
    }

    printf("[mpirun] Establishing Unix Domain Socket mesh...\n");

    for (int i = 0; i < num_procs; i++)
    {
        for (int j = i + 1; j < num_procs; j++)
        {
            int pair[2];

            if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) < 0)
            {
                perror("[mpirun] Fatal: socketpair failed");
                free(socket_mesh);
                exit(EXIT_FAILURE);
            }

            socket_mesh[i * num_procs + j] = pair[0];
            socket_mesh[j * num_procs + i] = pair[1];
        }
    }

    printf("[mpirun] Mesh topology successfully created!\n");

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

            // MAX int is 10 chars + 1 for comma + 1 for null terminator = 12 bytes per rank
            size_t fd_list_size = num_procs * 12;
            char *fd_list = malloc(fd_list_size);
            if (fd_list == NULL)
            {
                perror("[mpirun-chidl] Fatal: malloc failed for fd_list");
                exit(EXIT_FAILURE);
            }
            fd_list[0] = '\0';
            int my_rank = i;

            for (int r1 = 0; r1 < num_procs; r1++)
            {
                for (int r2 = r1 + 1; r2 < num_procs; r2++)
                {
                    int handset_A = socket_mesh[r1 * num_procs + r2];
                    int handset_B = socket_mesh[r2 * num_procs + r1];

                    if (my_rank == r1)
                    {
                        close(handset_B);
                        char temp[16];
                        snprintf(temp, sizeof(temp), "%d,", handset_A);
                        strcat(fd_list, temp);
                    }
                    else if (my_rank == r2)
                    {
                        close(handset_A);
                        char temp[16];
                        snprintf(temp, sizeof(temp), "%d,", handset_B);
                        strcat(fd_list, temp);
                    }
                    else
                    {
                        close(handset_A);
                        close(handset_B);
                    }
                }
            }

            setenv("MPI_SOCKET_FDS", fd_list, 1);

            execlp(target_executable, target_executable, NULL);
            perror("exec failed");

            free(fd_list);
            free(socket_mesh);

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

    // The parent doesn't participate in MPI messages
    for (int r1 = 0; r1 < num_procs; r1++)
    {
        for (int r2 = r1 + 1; r2 < num_procs; r2++)
        {
            close(socket_mesh[r1 * num_procs + r2]);
            close(socket_mesh[r2 * num_procs + r1]);
        }
    }

    free(socket_mesh);

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
