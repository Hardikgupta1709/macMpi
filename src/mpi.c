#include "../include/mpi.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>

// this struct is internal to our library

typedef struct
{
    int rank;
    int size;
    int *peer_sockets; // Dynamically allocated 1D array of inherited FD's
    int initialized;
} MPI_GlobalState;

typedef struct __attribute__((aligned(64)))
{
    uint32_t magic;
    int source;
    int dest;
    int tag;
    MPI_Datatype type;
    int count;
    size_t data_length;
    uint8_t padding[32];
} MPI_Header;

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

static int write_all(int fd, const void *buffer, size_t length)
{
    const char *ptr = (const char *)buffer;
    size_t bytes_left = length;

    while (bytes_left > 0)
    {
        ssize_t written = write(fd, ptr, bytes_left);
        if (written <= 0)
        {
            if (errno == EINTR)
            {
                continue; // Interrupted by a signal, try again
            }
            return -1;
        }
        bytes_left -= written;
        ptr += written; // Advanced the pointer forward by the amount sent
    }
    return 0;
}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    if (dest < 0 || dest >= g_mpi_state.size || dest == g_mpi_state.rank)
    {
        return MPI_ERR_OTHER;
    }

    int socket_fd = g_mpi_state.peer_sockets[dest];

    // calculating the payload size
    size_t type_size = (datatype == MPI_INT) ? sizeof(int) : 1;
    size_t payload_bytes = count * type_size;

    // constructing the 64-byte envelope
    MPI_Header header;
    header.magic = 0x4D504931;
    header.source = g_mpi_state.rank;
    header.dest = dest;
    header.tag = tag;
    header.type = datatype;
    header.count = count;
    header.data_length = payload_bytes;

    // pushing the envelope the across the socket
    if (write_all(socket_fd, &header, sizeof(MPI_Header)) != 0)
    {
        return MPI_ERR_OTHER;
    }

    if (write_all(socket_fd, buf, payload_bytes) != 0)
    {
        return MPI_ERR_OTHER;
    }

    return MPI_SUCCESS;
}