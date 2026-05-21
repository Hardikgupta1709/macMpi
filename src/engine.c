#include "mpi_internal.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <poll.h>
#include <unistd.h>

// macOS specific header
#include <sys/qos.h>

RequestQueue engine_queue = {NULL, NULL, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER};

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

        // Polling netwrok sockets for incoming data
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

        int ret = poll(fds, num_fds, 10);

        if (ret > 0)
        {
            for (int i = 0; i < num_fds; i++)
            {
                if (fds[i].revents & POLLIN)
                {
                    int active_fd = fds[i].fd;
                    MPI_Header incoming_header;

                    if (read(active_fd, &incoming_header, sizeof(MPI_Header)) > 0)
                    {
                        struct MPI_Request_int *waiting_req = match_active_receive(incoming_header.source, incoming_header.tag);

                        if (waiting_req != NULL)
                        {
                            printf("[Engine] Incoming message matched acive MPI_Irecv, routing to user buffer.\n");
                            read_all(active_fd, waiting_req->buffer, incoming_header.data_length);
                            waiting_req->is_complete = 1;
                        }
                        else
                        {
                            printf("[Engine] No active receive found. Routing to UMQ.\n");
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
        free(fds);
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