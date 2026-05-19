#include <pthread.h>
#ifndef MPI_INTERNAL_H
#define MPI_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include "../include/mpi.h"

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

extern MPI_GlobalState g_MPI_state;

int write_all(int fd, const void *buffer, size_t length);

typedef enum
{
    REQ_SEND,
    REQ_RECV
} RequestType;

struct MPI_Request_int
{
    RequestType type;
    int target_rank;
    int tag;
    void *buffer;
    size_t count;
    size_t datatype_size;

    volatile int is_complete;

    struct MPI_Request_int *next;
};

typedef struct
{
    struct MPI_Request_int *head;
    struct MPI_Request_int *tail;

    pthread_mutex_t mutex;
    pthread_cond_t cond;
} RequestQueue;

extern RequestQueue engine_queue;

void start_engine();
void stop_engine();

void enqueue_request(struct MPI_Request_int *req);

#endif