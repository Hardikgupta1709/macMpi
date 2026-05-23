#ifndef H_MPI
#define H_MPI

#define MPI_SUCCESS 0
#define MPI_ERR_OTHER -1
#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG -1

#include <stddef.h>

// Defining MPI_Comm as an integer handle so that it prevents the user form trying to access internal routing tables
typedef int MPI_Comm;
typedef int MPI_Datatype;
typedef struct MPI_Request_int *MPI_Request;

#define MPI_REQUEST_NULL ((MPI_Request)0)
typedef struct
{
    int MPI_SOURCE;
    int MPI_TAG;
    int MPI_ERROR;

    size_t _internal_count;
} MPI_Status;

#define MPI_STATUS_IGNORE ((MPI_Status *)0)
int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request);
int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request);
int MPI_Wait(MPI_Request *request, MPI_Status *status);
int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status);

#define MPI_INT 1
#define MPI_FLOAT 2
#define MPI_DOUBLE 3
#define MPI_CHAR 4

// predefined communicator to identify the world communicator
#define MPI_COMM_WORLD ((MPI_Comm)0x4444)

int MPI_Init(int *argc, char ***argv);
int MPI_Finalize(void);
int MPI_Comm_rank(MPI_Comm comm, int *rank);
int MPI_Comm_size(MPI_Comm comm, int *size);

int MPI_Send(const void *buff, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm);

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status);

int MPI_Barrier(MPI_Comm comm);

int MPI_Bcast(void *buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm);

#endif
