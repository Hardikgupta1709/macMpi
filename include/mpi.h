#ifndef H_MPI
#define H_MPI

#define MPI_SUCCESS 0
#define MPI_ERR_OTHER 1

// Defining MPI_Comm as an integer handle so that it prevents the user form trying to access internal routing tables
typedef int MPI_Comm;

// predefined communicator to identify the world communicator
#define MPI_COMM_WORLD ((MPI_Comm)0x4444)

int MPI_Init(int *argc, char ***argv);
int MPI_Finalize(void);
int MPI_Comm_rank(MPI_Comm comm, int *rank);
int MPI_Comm_size(MPI_Comm comm, int *size);

#endif
