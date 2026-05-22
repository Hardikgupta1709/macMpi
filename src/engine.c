#include "mpi_internal.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <poll.h>
#include <unistd.h>

// macOS specific header
#include <sys/qos.h>

RequestQueue engine_queue = {NULL, NULL, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, PTHREAD_COND_INITIALIZER};

volatile int engine_is_running = 0;
pthread_t progress_thread;

struct MPI_Request_int *active_receives_head = NULL;
struct MPI_Request_int *active_receives_tail = NULL;

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

    // Kqueue setup
    int kq = kqueue();
    if (kq == -1)
    {
        perror("[Engine] Fatal: kquque initializatio failed");
        exit(EXIT_FAILURE);
    }

    // register all peer sockets to the kqueue exactly once
    for (int i = 0; i < g_mpi_state.size; i++)
    {
        if (i != g_mpi_state.rank && g_mpi_state.peer_sockets[i] != -1)
        {
            struct kevent change;

            // macOS: keep eye on this socket for READ events, ADD it to the queue, and enable it
            EV_SET(&change, g_mpi_state.peer_sockets[i], EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);

            if (kevent(kq, &change, 1, NULL, 0, NULL) == -1)
            {
                perror("[Engine] Fatal: kevent socket registration failed");
                exit(EXIT_FAILURE);
            }
        }
    }

    printf("[Engine] kqueue successfully registered all peer sockets.\n");

    while (engine_is_running)
    {
        pthread_mutex_lock(&engine_queue.mutex);

        if (!engine_is_running && queue_is_empty())
        {
            pthread_mutex_unlock(&engine_queue.mutex);
            break;
        }

        struct MPI_Request_int *req = dequeue_request();

        pthread_mutex_unlock(&engine_queue.mutex);

        if (req != NULL)
        {
            if (req->type == REQ_SEND)
            {
                printf("[Engine] Processing SEND request for Target Rank %d...\n", req->target_rank);

                // Write the data to the network
                int dest_fd = g_mpi_state.peer_sockets[req->target_rank];
                size_t payload_bytes = req->count * req->datatype_size;

                MPI_Header header = {
                    .magic = 0x4D504931,
                    .source = g_mpi_state.rank,
                    .dest = req->target_rank,
                    .tag = req->tag,
                    .type = MPI_INT,
                    .count = req->count,
                    .data_length = payload_bytes};

                write_all(dest_fd, &header, sizeof(MPI_Header));
                write_all(dest_fd, req->buffer, payload_bytes);

                req->is_complete = 1;
                pthread_cond_broadcast(&engine_queue.completion_cond);
            }
            else if (req->type == REQ_RECV)
            {
                printf("[Engine] Processing RECV request from Source Rank %d...\n", req->target_rank);

                // part-1 two way matching (Backward)
                struct UMQ_Node *matched_node = extract_from_umq(req->target_rank, req->tag);

                if (matched_node != NULL)
                {
                    printf("[Engine] Match found in UMQ.\n");

                    // copying the payload from UMQ into the user waiting buffer
                    memcpy(req->buffer, matched_node->payload, matched_node->header.data_length);

                    free(matched_node->payload);
                    free(matched_node);

                    // marked request as complete so that MPI_Wait can unblock Main thread
                    req->is_complete = 1;
                    pthread_cond_broadcast(&engine_queue.completion_cond);
                }
                else
                {
                    // payload isn't in UMQ, waiting for it to arrive on the network
                    printf("[Engine] not in UMQ, moving to active Receives list...\n");

                    // unlinked from the main thread queue
                    req->next = NULL;

                    // appending to the engine's internal active Receives list
                    if (active_receives_head == NULL)
                    {
                        active_receives_head = req;
                        active_receives_tail = req;
                    }
                    else
                    {
                        active_receives_tail->next = req;
                        active_receives_tail = req;
                    }
                }
            }
        }

        struct kevent eventlist[32]; // processing upto 32 network events at once.
        struct timespec timeout = {0, 10000000};

        // block until data arrives or 10ms pass
        int num_events = kevent(kq, NULL, 0, eventlist, 32, &timeout);

        if (num_events > 0)
        {
            for (int i = 0; i < num_events; i++)
            {
                // kqueue stores the socket file descriptors inside the 'ident' field
                int active_fd = (int)eventlist[i].ident;
                MPI_Header incoming_header;

                if (read(active_fd, &incoming_header, sizeof(MPI_Header)) > 0)
                {
                    struct MPI_Request_int *waiting_req = match_active_receive(incoming_header.source, incoming_header.tag);

                    if (waiting_req != NULL)
                    {
                        printf("[Engine] kqueue event: Matched active MPI_Irecv, routing to user.\n");
                        read_all(active_fd, waiting_req->buffer, incoming_header.data_length);

                        waiting_req->is_complete = 1;
                        pthread_cond_broadcast(&engine_queue.completion_cond); // wake up mpi wait
                    }
                    else
                    {
                        printf("[Engine] kqueue event: No active receive. Routing to UMQ.\n");
                        struct UMQ_Node *new_node = malloc(sizeof(struct UMQ_Node));
                        new_node->header = incoming_header;
                        new_node->payload = malloc(incoming_header.data_length);
                        read_all(active_fd, new_node->payload, incoming_header.data_length);
                        new_node->next = NULL;

                        if (g_mpi_state.umq_head == NULL)
                        {
                            g_mpi_state.umq_head = g_mpi_state.umq_tail = new_node;
                        }
                        else
                        {
                            g_mpi_state.umq_tail->next = new_node;
                            g_mpi_state.umq_tail = new_node;
                        }
                    }
                }
            }
        }
    }
    close(kq);
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

void enqueue_request(struct MPI_Request_int *req)
{
    pthread_mutex_lock(&engine_queue.mutex);

    req->next = NULL;
    if (engine_queue.tail == NULL)
    {
        engine_queue.head = req;
        engine_queue.tail = req;
    }
    else
    {
        engine_queue.tail->next = req;
        engine_queue.tail = req;
    }

    pthread_cond_signal(&engine_queue.cond);

    pthread_mutex_unlock(&engine_queue.mutex);
}