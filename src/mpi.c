#include "../include/mpi.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

// this struct is internal to our library

typedef struct
{
    int rank;
    int size;
    int *peer_sockets; // Dynamically allocated 1D array of inherited FD's
    int initialized;
} MPI_GlobalState;

MPI_GlobalState g_mpi_state = {-1, -1, NULL, 0};

int MPI_Init(int *argc, char ***argv)
{
    if (g_mpi_state.initialized)
    {
        return MPI_SUCCESS;
    }

    // Reading environment variables injected by mpirun
    char *rank_str = getenv("MPI_RANK");
    char *size_str = getenv("MPI_UNIVERSE_SIZE");
    char *fds_str = getenv("MPI_SOCKET_FDS");

    if (!rank_str || !size_str || !fds_str)
    {
        fprintf(stderr, "MPI Fatal: Not launched via mpirun. Missing environment.\n");
        exit(1);
    }

    g_mpi_state.rank = atoi(rank_str);
    g_mpi_state.size = atoi(size_str);

    // Allocating the routing table safely on the heap
    g_mpi_state.peer_sockets = (int *)malloc(g_mpi_state.size * sizeof(int));
    if (!g_mpi_state.peer_sockets)
    {
        fprintf(stderr, "MPI Fatal: out of memory.\n");
        exit(1);
    }

    // Parsing the comma-separated socket string

    char *token = strtok(fds_str, ",");

    for (int i = 0; i < g_mpi_state.size; i++)
    {
        if (i == g_mpi_state.rank)
        {
            // No socket -> self talk
            g_mpi_state.peer_sockets[i] = -1;
        }
        else
        {
            // storing the parsed file descriptor and grab the next one
            if (token != NULL)
            {
                g_mpi_state.peer_sockets[i] = atoi(token);
                token = strtok(NULL, ",");
            }
            else
            {
                fprintf(stderr, "MPI Fatal: Rank %d did not recieve enough sockets.\n", g_mpi_state.rank);
                exit(1);
            }
        }
    }

    g_mpi_state.initialized = 1;
    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }
    *rank = g_mpi_state.rank;
    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm comm, int *size)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }
    *size = g_mpi_state.size;
    return MPI_SUCCESS;
}

int MPI_Finalize(void)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_SUCCESS;
    }

    for (int i = 0; i < g_mpi_state.size; i++)
    {
        if (g_mpi_state.peer_sockets[i] != -1)
        {
            close(g_mpi_state.peer_sockets[i]);
        }
    }

    free(g_mpi_state.peer_sockets);
    g_mpi_state.initialized = 0;

    return MPI_SUCCESS;
}