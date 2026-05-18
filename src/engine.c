#include "mpi_internal.h"
#include <stdlib.h>
#include <stdio.h>

// macOS specific header
#include <sys/qos.h>

RequestQueue engine_queue = {NULL, NULL, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};

volatile int engine_is_running = 0;
pthread_t progress_thread;

// Queue healpers

static int queue_is_empty()
{
    return engine_queue.head == NULL;
}

// Remove a request from the front of the queue
static struct MPI_Request_int *dequeue_request()
{
    struct MPI_Request_int *req = engine_queue.head;
    if (req != NULL)
    {
        engine_queue.head = req->next;
        if (engine_queue.head == NULL)
        {
            engine_queue.tail = NULL;
        }
    }
    return req;
}

void *progress_engine_loop(void *arg)
{
    // Hardware optimisation: Pin this thread to Apple's Performance cores

    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);

    printf("[Engine] Background Thread started on Performance Cores.\n");

    while (engine_is_running)
    {
        pthread_mutex_lock(&engine_queue.mutex);

        while (queue_is_empty() && engine_is_running)
        {
            pthread_cond_wait(&engine_queue.cond, &engine_queue.mutex);
        }

        if (!engine_is_running && queue_is_empty())
        {
            pthread_mutex_unlock(&engine_queue.mutex);
            break;
        }

        struct MPI_Request_int *req = dequeue_request();

        pthread_mutex_unlock(&engine_queue.mutex);

        if (req != NULL)
        {
            printf("[Engine] Processing request for Target Rank %d...\n", req->target_rank);

            req->is_complete = 1;
        }
    }
    printf("[Engine] Background thread shutting down.\n");
    return NULL;
}

void start_engine()
{
    engine_is_running = 1;

    if (pthread_create(&progress_thread, NULL, progress_engine_loop, NULL) != 0)
    {
        fprintf(stderr, "[MPI Error] Failed to start background progress engine.\n");
        exit(EXIT_FAILURE);
    }
}

void stop_engine()
{
    pthread_mutex_lock(&engine_queue.mutex);

    engine_is_running = 0;

    pthread_cond_signal(&engine_queue.cond);

    pthread_mutex_unlock(&engine_queue.mutex);

    pthread_join(progress_thread, NULL);
}