#include <pthread.h>
#include "../include/mpi.h"

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
