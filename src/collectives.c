#include "mpi.h"
#include "mpi_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// Internal library tag to prevent collision with user messages
#define MPI_TAG_BARRIER 9999

#define SAFE_MOD(a, b) (((a) % (b)) + (b) % (b))

// calculates ceil(log2(size)) using bitwise integer math, runs in ALU in less than a nanosecond
static inline int get_num_rounds(int size)
{
    int rounds = 0;

    while ((1 << rounds) < size)
    {
        rounds++;
    }
    return rounds;
}

// INTERNAL BACKEND ENGINE

static int barrier_dissemination(MPI_Comm comm)
{
    int size, rank;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);

    int max_rounds = get_num_rounds(size);

    int dummy = 0;
    MPI_Request req;

    // 1. (log2 N) decentralized peer-to-peer synchronization
    for (int k = 0; k < max_rounds; k++)
    {
        int jump = 1 << k;

        // OPTION_3 : ZERO-MODULE fast path
        int send_to = rank + jump;
        if (send_to >= size)
        {
            send_to -= size;
        }

        int recv_from = rank - jump;
        if (recv_from < 0)
        {
            recv_from += size;
        }

        // 1. Dispatching the non-blocking send
        MPI_Isend(&dummy, 0, MPI_INT, send_to, MPI_TAG_BARRIER, comm, &req);

        // 2. Block and wait for required incoming signal
        MPI_Recv(&dummy, 0, MPI_INT, recv_from, MPI_TAG_BARRIER, comm, MPI_STATUS_IGNORE);

        // 3. ensuring our outbound message successfully left the socket
        MPI_Wait(&req, MPI_STATUS_IGNORE);
    }
    return MPI_SUCCESS;
}

static int barrier_tree(MPI_Comm comm)
{
    // 2. O(N) heirarchial parent-child synchronization

    return MPI_SUCCESS;
}

// THE PUBLIC DISPATCHER

int MPI_Barrier(MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        fprintf(stderr, "[MPI Fatal] Cannot call barrier before MPI_Init.\n");
        return MPI_ERR_OTHER;
    }

    int size;
    MPI_Comm_size(comm, &size);

    if (size <= 1)
    {
        return MPI_SUCCESS;
    }

    // Lazy initialisation instead of harcoded value
    static int threshold = -1;
    if (threshold == -1)
    {
        char *env_thresh = getenv("MPI_BARRIER_THRESHOLD");
        if (env_thresh != NULL)
        {
            threshold = atoi(env_thresh); // Admin override
        }
        else
        {
            threshold = 16;
        }
    }

    if (size < threshold)
    {
        return barrier_dissemination(comm);
    }
    else
    {
        return barrier_tree(comm);
    }
}
