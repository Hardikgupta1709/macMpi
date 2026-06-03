#include "../include/mpi.h"
#include "mpi_internal.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <poll.h>
#include <stddef.h>
#include <sched.h>

MPI_GlobalState g_mpi_state = {-1, -1, NULL, 0, NULL};

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

    start_engine();

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

    stop_engine();

    free(g_mpi_state.peer_sockets);
    g_mpi_state.initialized = 0;

    return MPI_SUCCESS;
}

int write_all(int fd, const void *buffer, size_t length)
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
        return MPI_ERR_OTHER;

    MPI_Request req;

    // 1. Push the payload to the Engine's outbound queue
    MPI_Isend(buf, count, datatype, dest, tag, comm, &req);

    // 2. Wait for the background thread to finish the socket transfer
    MPI_Wait(&req, MPI_STATUS_IGNORE);

    return MPI_SUCCESS;
}

// Internal Matching logic
static int is_match(int req_source, int req_tag, int hdr_source, int hdr_tag)
{
    // Matches from the user if it asked for this exact source
    int source_matches = (req_source == MPI_ANY_SOURCE) || (req_source == hdr_source);

    // Matches for the exact tag that user asked
    int tag_matches = (req_tag == MPI_ANY_TAG) || (req_tag == hdr_tag);

    return source_matches && tag_matches;
}

// Loops until the exact number of bytes requested is pulled fromt the os
int read_all(int fd, void *buffer, size_t length)
{
    char *ptr = (char *)buffer;
    size_t bytes_left = length;

    while (bytes_left > 0)
    {
        ssize_t bytes_read = read(fd, ptr, bytes_left);
        if (bytes_read < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return -1; // Actual Network error
        }
        else if (bytes_read == 0)
        {
            return -1; // Sender disconnected unexpectedly
        }
        bytes_left -= bytes_read;
        ptr += bytes_read;
    }
    return 0;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status)
{
    if (!g_mpi_state.initialized)
        return MPI_ERR_OTHER;

    MPI_Request req;

    // 1. Dispatch the request to the Engine's Two-Way Matching system
    MPI_Irecv(buf, count, datatype, source, tag, comm, &req);

    // 2. Safely put the Main Thread to sleep until the Engine wakes it up
    MPI_Wait(&req, status);

    return MPI_SUCCESS;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    // Allocating the internal request structure on the heap
    struct MPI_Request_int *internal_req = malloc(sizeof(struct MPI_Request_int));
    if (!internal_req)
    {
        fprintf(stderr, "MPI Fatal: Out of memory allocating request.\n");
        return MPI_ERR_OTHER;
    }

    internal_req->type = REQ_SEND;
    internal_req->target_rank = dest;
    internal_req->tag = tag;

    internal_req->buffer = (void *)buf;

    internal_req->count = count;
    internal_req->datatype_size = (datatype == MPI_INT) ? sizeof(int) : 1;
    internal_req->next = NULL;
    internal_req->is_complete = 0;

    printf("[Main] MPI_Isend called: Packaged request for rank %d (Tag %d). Pushing to Engine...\n", dest, tag);

    enqueue_request(internal_req);

    *request = internal_req;

    return MPI_SUCCESS;
}

int MPI_Wait(MPI_Request *request, MPI_Status *status)
{
    if (request == NULL || *request == MPI_REQUEST_NULL)
    {
        return MPI_SUCCESS;
    }

    struct MPI_Request_int *req = *request;

    // replaced spin-loop with spurious-wakeup-safe condition wait
    pthread_mutex_lock(&engine_queue.mutex);

    // Spin wait loop
    while (req->is_complete == 0)
    {
        pthread_cond_wait(&engine_queue.completion_cond, &engine_queue.mutex);
    }

    pthread_mutex_unlock(&engine_queue.mutex);

    // Background work is finished -> memory clean up
    free(req);
    *request = MPI_REQUEST_NULL;

    return MPI_SUCCESS;
}

int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status)
{
    if (request == NULL || *request == MPI_REQUEST_NULL)
    {
        *flag = 1;
        return MPI_SUCCESS;
    }

    struct MPI_Request_int *req = *request;

    pthread_mutex_lock(&engine_queue.mutex);
    *flag = req->is_complete;
    pthread_mutex_unlock(&engine_queue.mutex);

    if (*flag)
    {
        free(req);
        *request = MPI_REQUEST_NULL;
    }
    return MPI_SUCCESS;
}
int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request)
{
    if (!g_mpi_state.initialized)
    {
        return MPI_ERR_OTHER;
    }

    struct MPI_Request_int *req = malloc(sizeof(struct MPI_Request_int));
    if (!req)
    {
        return MPI_ERR_OTHER;
    }

    req->type = REQ_RECV;
    req->target_rank = source;
    req->tag = tag;
    req->buffer = buf;
    req->count = (size_t)count;
    req->datatype_size = (datatype == MPI_INT) ? sizeof(int) : 1;
    req->next = NULL;
    req->is_complete = 0;

    enqueue_request(req);

    *request = req;

    return MPI_SUCCESS;
}

struct UMQ_Node *extract_from_umq(int source, int tag)
{
    struct UMQ_Node *current = g_mpi_state.umq_head;
    struct UMQ_Node *prev = NULL;

    while (current != NULL)
    {
        if (is_match(source, tag, current->header.source, current->header.tag))
        {
            // if we found teh match then unlink from the queue
            if (prev == NULL)
            {
                // match found at front of queue.
                g_mpi_state.umq_head = current->next;
            }
            else
            {
                // match in middle.
                prev->next = current->next;
            }
            if (current == g_mpi_state.umq_tail)
            {
                g_mpi_state.umq_tail = prev;
            }

            // the unlinked node
            return current;
        }

        prev = current;
        current = current->next;
    }

    return NULL;
}

struct MPI_Request_int *match_active_receive(int source, int tag)
{
    struct MPI_Request_int *current = active_receives_head;
    struct MPI_Request_int *prev = NULL;

    while (current != NULL)
    {
        if (is_match(source, tag, current->target_rank, current->tag))
        {

            if (prev == NULL)
            {
                active_receives_head = current->next;
            }
            else
            {
                prev->next = current->next;
            }

            if (current == active_receives_tail)
            {
                active_receives_tail = prev;
            }
            return current;
        }
        prev = current;
        current = current->next;
    }
    return NULL;
}
