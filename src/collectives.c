#include "mpi.h"
#include "mpi_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// Internal library tag to prevent collision with user messages
#define MPI_TAG_BARRIER 9999

#define MPI_TAG_BCAST 9998

#define SAFE_MOD(a, b) (((a) % (b) + (b)) % (b))

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
        int send_to = (rank + jump) % size;
        int recv_from = (rank - jump + size) % size;

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
    int size, rank;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);

    int dummy = 0;
    MPI_Request req_left, req_right, req_parent;

    int left_child = (2 * rank) + 1;
    int right_child = (2 * rank) + 2;
    int parent = (rank - 1) / 2;

    // 1. arrival at the barrier

    // wait for the left child to arrive
    if (left_child < size)
    {
        MPI_Recv(&dummy, 0, MPI_INT, left_child, MPI_TAG_BARRIER, comm, MPI_STATUS_IGNORE);
    }

    // wait for the right child to arrive
    if (right_child < size)
    {
        MPI_Recv(&dummy, 0, MPI_INT, right_child, MPI_TAG_BARRIER, comm, MPI_STATUS_IGNORE);
    }

    if (rank != 0)
    {
        MPI_Isend(&dummy, 0, MPI_INT, parent, MPI_TAG_BARRIER, comm, &req_parent);
        MPI_Wait(&req_parent, MPI_STATUS_IGNORE);
    }

    // 2. release form the barrier

    // wait for root to release
    if (rank != 0)
    {
        MPI_Recv(&dummy, 0, MPI_INT, parent, MPI_TAG_BARRIER, comm, MPI_STATUS_IGNORE);
    }

    // passing the release signal down to the left child
    if (left_child < size)
    {
        MPI_Isend(&dummy, 0, MPI_INT, left_child, MPI_TAG_BARRIER, comm, &req_left);
        MPI_Wait(&req_left, MPI_STATUS_IGNORE);
    }

    // passign the release signal down to the right child
    if (right_child < size)
    {
        MPI_Isend(&dummy, 0, MPI_INT, right_child, MPI_TAG_BARRIER, comm, &req_right);
        MPI_Wait(&req_right, MPI_STATUS_IGNORE);
    }

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

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    int size, rank;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);

    if (size <= 1)
    {
        return MPI_SUCCESS;
    }

    // mapped actual ranks to virtual ranks so that root is always mathematically '0'
    int v_rank = (rank - root + size) % size;
    int max_rounds = get_num_rounds(size);

    // the binomial tree geometric progression
    for (int i = 0; i < max_rounds; i++)
    {
        int tag = MPI_TAG_BCAST + i;
        int jump = 1 << i; // 2^i

        if (v_rank < jump)
        {
            // Already having the data, aacting as sender
            int dest_v = v_rank + jump;

            // safety check for not sending to ghost node if size not a power of 2
            if (dest_v < size)
            {
                // converting the virtual back to a real rank
                int dest = (dest_v + root) % size;

                MPI_Request req;

                MPI_Isend(buffer, count, datatype, dest, tag, comm, &req);
                MPI_Wait(&req, MPI_STATUS_IGNORE);
            }
        }
        else if (v_rank >= jump && v_rank < (jump * 2))
        {
            // time to receive
            int source_v_rank = v_rank - jump;
            int source = (source_v_rank + root) % size;

            MPI_Request recv_req;
            // 1. Initiate non-blocking recieve
            MPI_Irecv(buffer, count, datatype, source, tag, comm, &recv_req);
            // 2. wait by main thread until the engine has finished copying the data
            MPI_Wait(&recv_req, MPI_STATUS_IGNORE);
        }
    }
    return MPI_SUCCESS;
}