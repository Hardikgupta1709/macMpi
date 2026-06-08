#include "mpi.h"
#include "mpi_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

// Internal library tag to prevent collision with user messages
#define MPI_TAG_BARRIER 9999

#define MPI_TAG_BCAST 9998

#define MPI_TAG_GATHER 9996

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

        int round_tag = MPI_TAG_BARRIER + k;

        // 1. Dispatching the non-blocking send
        MPI_Isend(&dummy, 0, MPI_INT, send_to, round_tag, comm, &req);

        // 2. Block and wait for required incoming signal
        MPI_Recv(&dummy, 0, MPI_INT, recv_from, round_tag, comm, MPI_STATUS_IGNORE);

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

// Internal helper for MPI_Reduce

static void apply_reduction_op(const void *inbuf, void *inoutbuf, int count, MPI_Datatype datatype, MPI_Op op)
{
    if (datatype == MPI_INT)
    {
        const int *in = (const int *)inbuf;
        int *inout = (int *)inoutbuf;

        for (int i = 0; i < count; i++)
        {
            switch (op)
            {
            case MPI_SUM:
                inout[i] += in[i];
                break;
            case MPI_PROD:
                inout[i] *= in[i];
                break;
            case MPI_MAX:
                if (in[i] > inout[i])
                {
                    inout[i] = in[i];
                }
                break;
            case MPI_MIN:
                if (in[i] < inout[i])
                {
                    inout[i] = in[i];
                }
                break;
            }
        }
    }
    else if (datatype == MPI_DOUBLE)
    {
        const double *in = (const double *)inbuf;
        double *inout = (double *)inoutbuf;

        for (int i = 0; i < count; i++)
        {
            switch (op)
            {
            case MPI_SUM:
                inout[i] += in[i];
                break;
            case MPI_PROD:
                inout[i] *= in[i];
                break;
            case MPI_MAX:
                if (in[i] > inout[i])
                {
                    inout[i] = in[i];
                }
                break;
            case MPI_MIN:
                if (in[i] > inout[i])
                {
                    inout[i] = in[i];
                }
                break;
            }
        }
    }
}

static int get_dt_size(MPI_Datatype dt)
{
    if (dt == MPI_INT)
    {
        return sizeof(int);
    }
    if (dt == MPI_FLOAT)
    {
        return sizeof(float);
    }
    if (dt == MPI_DOUBLE)
    {
        return sizeof(double);
    }
    return 1;
}

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    int size, rank;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);

    // virtualised topology so root acts as rank 0
    int v_rank = (rank - root + size) % size;
    int v_left = (2 * v_rank) + 1;
    int v_right = (2 * v_rank + 2);
    int v_parent = (v_rank - 1) / 2;

    // Physical targets
    int p_left = (v_left + root) % size;
    int p_right = (v_right + root) % size;
    int p_parent = (v_parent + root) % size;

    // buffer management
    int byte_length = count * get_dt_size(datatype);

    // work_bf starts as a copy of this process's own sendbuf
    void *work_buf = malloc(byte_length);
    memcpy(work_buf, sendbuf, byte_length);

    // temp_recv is used strictly to catch incoming data form children
    void *temp_recv = malloc(byte_length);
    MPI_Request req;
    MPI_Status status;

    // data in phase ->bottom up

    // A. wait for the left child
    if (v_left < size)
    {
        MPI_Irecv(temp_recv, count, datatype, p_left, 9997, comm, &req);
        MPI_Wait(&req, &status);

        apply_reduction_op(temp_recv, work_buf, count, datatype, op);
    }

    // B. wait for the right child
    if (v_right < size)
    {
        MPI_Irecv(temp_recv, count, datatype, p_right, 9997, comm, &req);
        MPI_Wait(&req, &status);
        apply_reduction_op(temp_recv, work_buf, count, datatype, op);
    }

    // C. forward up to Parent or to user's recvbuf
    if (v_rank != 0)
    {
        // Not the root: send the accumulated work_buf up the tree
        MPI_Isend(work_buf, count, datatype, p_parent, 9997, comm, &req);
        MPI_Wait(&req, &status);
    }
    else
    {
        // the root
        // copying to user's provided recvbuf
        if (recvbuf != NULL)
        {
            memcpy(recvbuf, work_buf, byte_length);
        }
    }

    free(work_buf);
    free(temp_recv);

    return MPI_SUCCESS;
}

int MPI_Scatter(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    // Fetch current process state

    int rank, size;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    if (sendcount != recvcount || get_dt_size(sendtype) != get_dt_size(recvtype))
    {
        // symmetric scatter counts
        return MPI_ERR_TRUNCATE;
    }

    size_t chunk_size_bytes = sendcount * get_dt_size(sendtype);

    if (rank == root)
    {
        // Root process : the asynchronous burst

        // Allocating request tracker for non blocking engine
        MPI_Request *reqs = (MPI_Request *)malloc(sizeof(MPI_Request) * size);
        int req_count = 0;

        char *base_send_ptr = (char *)sendbuf;

        for (int i = 0; i < size; i++)
        {
            if (i == root)
            {
                // Local copy for the root itself
                memcpy(recvbuf, base_send_ptr + (i * chunk_size_bytes), chunk_size_bytes);
            }
            else
            {
                // exact memory offset for process 'i
                void *target_payload = base_send_ptr + (i * chunk_size_bytes);

                MPI_Isend(target_payload, sendcount, sendtype, i, MPI_TAG_SCATTER, comm, &reqs[req_count]);
                req_count++;
            }
        }

        // Waiting for the background engine to drain all the data to the socket pair

        MPI_Status status;
        for (int i = 0; i < req_count; i++)
        {
            MPI_Wait(&reqs[i], &status);
        }

        free(reqs);
    }
    else
    {
        // Leaf processes -> wait for data

        MPI_Status status;
        MPI_Recv(recvbuf, recvcount, recvtype, root, MPI_TAG_SCATTER, comm, &status);
    }
    return MPI_SUCCESS;
}

int MPI_Gather(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, int root, MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    int size, rank;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);

    if (rank == root)
    {
        size_t extent = get_dt_size(recvtype) * recvcount;

        MPI_Request *reqs = (MPI_Request *)malloc(sizeof(MPI_Request) * size);

        for (int i = 0; i < size; i++)
        {
            char *recv_offset = (char *)recvbuf + (i * extent);

            if (i == root)
            {
                memcpy(recv_offset, sendbuf, sendcount * get_dt_size(sendtype));
                reqs[i] = MPI_REQUEST_NULL;
            }
            else
            {
                MPI_Irecv(recv_offset, recvcount, recvtype, i, MPI_TAG_GATHER, comm, &reqs[i]);
            }
        }

        for (int i = 0; i < size; i++)
        {
            if (reqs[i] != MPI_REQUEST_NULL)
            {
                MPI_Wait(&reqs[i], MPI_STATUS_IGNORE);
            }
        }

        free(reqs);
    }
    else
    {
        MPI_Request req;
        MPI_Isend(sendbuf, sendcount, sendtype, root, MPI_TAG_GATHER, comm, &req);
        MPI_Wait(&req, MPI_STATUS_IGNORE);
    }
    return MPI_SUCCESS;
}

int MPI_Allgather(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    int rank, size;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);

    // gathering all the data to an internal root
    int err = MPI_Gather(sendbuf, sendcount, sendtype, recvbuf, recvcount, recvtype, 0, comm);

    if (err != MPI_SUCCESS)
    {
        return err;
    }

    // total payload size to broadcast
    int total_elements = recvcount * size;

    // bcast the fully concatenated array from internal root to every other root
    err = MPI_Bcast(recvbuf, total_elements, recvtype, 0, comm);

    return err;
}

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, MPI_Comm comm)
{
    if (!g_mpi_state.initialized)
    {
        fprintf(stderr, "[MPI Fatal] Cannot call MPI_Allreduce before MPI_Init.\n");
        return MPI_ERR_OTHER;
    }

    // Using rank 0 as the convergence point for the inverse tree
    int reduce_status = MPI_Reduce(sendbuf, recvbuf, count, datatype, op, 0, comm);

    if (reduce_status != MPI_SUCCESS)
    {
        return reduce_status;
    }

    // now rank 0 has final answer in recvbuf , we broadcast it
    int bcast_status = MPI_Bcast(recvbuf, count, datatype, 0, comm);

    return bcast_status;
}

int MPI_Alltoall(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, int recvcount, MPI_Datatype recvtype, MPI_Comm comm)
{
    int rank, size;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    size_t send_bytes = sendcount * sizeof(int);
    size_t recv_bytes = recvcount * sizeof(int);

    int num_requests = 2 * (size - 1);
    MPI_Request *requests = malloc(num_requests * sizeof(MPI_Request));
    int req_idx = 0;

    // 1. The self copy -> not directly sending data over socket to ourselves, just doing a direct self copy
    void *recv_target = (char *)recvbuf + (rank * recv_bytes);
    const void *send_source = (const char *)sendbuf + (rank * send_bytes);
    memcpy(recv_target, send_source, send_bytes);

    // 2. Post for all non-blocking receives first
    for (int i = 0; i < size; i++)
    {
        if (i == rank)
        {
            continue; // skip ourselves
        }

        // Exactly where in the receive buffer this incoming chunk belongs
        void *recv_offset = (char *)recvbuf + (i * recv_bytes);

        MPI_Irecv(recv_offset, recvcount, recvtype, i, MPI_TAG_ALLTOALL, comm, &requests[req_idx++]);
    }

    // 3. Post all the Non-Blocking sends
    for (int i = 0; i < size; i++)
    {
        if (i == rank)
        {
            continue;
        }

        const void *send_offset = (const char *)sendbuf + (i * send_bytes);

        // Pushing the transfer to the background again
        MPI_Isend(send_offset, sendcount, sendtype, i, MPI_TAG_ALLTOALL, comm, &requests[req_idx++]);
    }

    // 4. synchronize the whole thing i.e. blocking the main thread until kqueue engine finishes all 2*(N-1) transfers
    for (int i = 0; i < num_requests; i++)
    {
        MPI_Wait(&requests[i], MPI_STATUS_IGNORE);
    }

    free(requests);
    return MPI_SUCCESS;
}