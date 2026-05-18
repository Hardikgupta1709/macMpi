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

typedef struct UMQ_Node
{
    MPI_Header header;     // The routing envelope
    void *payload;         // Dyanmically Allocating buffer for the acutal data
    struct UMQ_Node *next; // Pointer to the next unexpected message
} UMQ_Node;

typedef struct
{
    int rank;
    int size;
    int *peer_sockets; // Dynamically allocated 1D array of inherited FD's
    int initialized;

    UMQ_Node *umq_head; // Head of the unexpected Message Queue
} MPI_GlobalState;

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
static int read_all(int fd, void *buffer, size_t length)
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
    {
        return MPI_ERR_OTHER;
    }

    // 1. Checking the Unexpected message queue
    UMQ_Node *curr = g_mpi_state.umq_head;
    UMQ_Node *prev = NULL;

    while (curr != NULL)
    {
        if (is_match(source, tag, curr->header.source, curr->header.tag))
        {
            // Found a Match in queue

            memcpy(buf, curr->payload, curr->header.data_length); // message copy from heap memory to user buffer

            if (status != NULL)
            {
                status->MPI_SOURCE = curr->header.source;
                status->MPI_TAG = curr->header.tag;
                status->MPI_ERROR = MPI_SUCCESS;
                status->_internal_count = curr->header.data_length;
            }

            // unlinking this node from the linked list
            if (prev == NULL)
            {
                g_mpi_state.umq_head = curr->next;
            }
            else
            {
                prev->next = curr->next;
            }

            free(curr->payload);
            free(curr);

            return MPI_SUCCESS;
        }

        // moving to next node if their in no match
        prev = curr;
        curr = curr->next;
    }

    // 2. Draining the Sockets

    while (1)
    {
        int active_fd = -1;

        // Determining which socket to read from
        if (source >= 0)
        {
            active_fd = g_mpi_state.peer_sockets[source];
        }
        else
        {
            // source  == MPI_ANY_SOURCE (Watching all the sockets for the message)

            struct pollfd *fds = malloc(g_mpi_state.size * sizeof(struct pollfd));
            int num_fds = 0;

            for (int i = 0; i < g_mpi_state.size; i++)
            {
                if (i != g_mpi_state.rank && g_mpi_state.peer_sockets[i] != -1)
                {
                    fds[num_fds].fd = g_mpi_state.peer_sockets[i];
                    fds[num_fds].events = POLLIN;
                    num_fds++;
                }
            }

            // Blocking the process until at least one socket has data
            int ret = poll(fds, num_fds, -1);
            if (ret < 0)
            {
                free(fds);
                return MPI_ERR_OTHER;
            }

            for (int i = 0; i < num_fds; i++)
            {
                if (fds[i].revents & POLLIN)
                {
                    active_fd = fds[i].fd;
                    break;
                }
            }
            free(fds);
        }

        MPI_Header incoming_header;

        // Reading the 64-Byte envelope
        read_all(active_fd, &incoming_header, sizeof(MPI_Header));

        if (is_match(source, tag, incoming_header.source, incoming_header.tag))
        {

            read_all(active_fd, buf, incoming_header.data_length);

            if (status != NULL)
            {
                status->MPI_SOURCE = incoming_header.source;
                status->MPI_TAG = incoming_header.tag;
                status->MPI_ERROR = MPI_SUCCESS;
                status->_internal_count = incoming_header.data_length;
            }
            return MPI_SUCCESS;
        }
        else
        {
            // NO Match, some another ones message

            // Draining the socket to prevent deadlock and save to UMQ;

            void *unexpected_payload = malloc(incoming_header.data_length);
            read_all(active_fd, unexpected_payload, incoming_header.data_length);

            // creating the new linked list node
            UMQ_Node *new_node = malloc(sizeof(UMQ_Node));
            new_node->header = incoming_header;
            new_node->payload = unexpected_payload;
            new_node->next = NULL;

            // appending to the end of UMQ
            if (g_mpi_state.umq_head == NULL)
            {
                g_mpi_state.umq_head = new_node;
            }
            else
            {
                UMQ_Node *tail = g_mpi_state.umq_head;
                while (tail->next != NULL)
                {
                    tail = tail->next;
                }
                tail->next = new_node;
            }
        }
    }
}