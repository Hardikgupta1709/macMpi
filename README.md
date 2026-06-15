# macMPI: Production-Grade MPI for Apple Silicon

![Language](https://img.shields.io/badge/Language-C11-blue.svg)
![Platform](https://img.shields.io/badge/Platform-macOS%20%28M--Series%29-black.svg)
![Build](https://img.shields.io/badge/Build-CMake%20%2B%20Ninja-success.svg)

**macMPI** (formerly _mpi-lite_) is a high-performance, single-node implementation of the MPI-1.1 standard built from scratch. Designed explicitly for macOS and optimized for Apple Silicon (M-Series), this project bypasses the shortcuts of a "toy library" to provide a production-hardened distributed systems environment.

It features a custom process manager, a full-mesh Unix Domain Socket topology, and a highly concurrent, zero-copy, non-blocking progress engine powered by native macOS `kqueue`.

## Architectural Milestones & Features

### 1. Process Management (`mpirun`)

A standalone daemon engineered to securely spawn, manage, and tear down a universe of distributed clones.

- **Environment Injection:** Safe `fork()` and `execvp()` boundaries with dynamic environment variable injection (e.g., `MPI_RANK`, `MPI_UNIVERSE_SIZE`).
- **I/O Multiplexing:** Completely eliminates Head-of-Line blocking. Captures all child output via POSIX Pipes (`pipe()`, `dup2()`) and routes it through an event-driven `poll()` loop to the terminal.
- **Graceful Teardown:** Advanced signal handlers (`SIGINT`, `SIGCHLD`) to intercept user terminations, safely assassinate child clones, and prevent zombie/orphaned processes.

### 2. Point-to-Point Communication

A robust data-movement layer built over a full-duplex Unix Domain Socket mesh.

- **Smart Envelope Plumbing:** 64-byte routing headers to ensure precise network delivery.
- **Kernel-Safe Transfers:** Strict return-value checking and pointer arithmetic to handle partial `write()` returns and OS socket buffer bottlenecks.
- **Wildcard & UMQ Engine:** Full support for `MPI_ANY_SOURCE` and `MPI_ANY_TAG` with an internal Unexpected Message Queue (UMQ) that safely buffers out-of-order packets without crashing the kernel.

### 3. The Non-Blocking Progress Engine (The Crown Jewel)

True compute-communication overlap. This multithreaded backend ensures the user's main program can crunch math while gigabytes of data transfer in the background.

- **Hardware-Optimized Threads:** A shadow `pthread` physically pinned to Apple Silicon's Performance Cores using `pthread_set_qos_class_self_np`.
- **$O(1)$ Event Polling:** Ripped out the slow `poll()` loop and implemented native macOS `kqueue()` / `kevent()`, allowing the background thread to monitor hundreds of sockets with zero CPU overhead.
- **Two-Way Matching:** Implements `MPI_Isend`, `MPI_Irecv`, and `MPI_Wait`, utilizing thread-safe linked lists protected by Mutexes and Condition Variables.

### 4. Advanced Collective Operations

Decentralized, mathematically optimized network topologies that eliminate the $O(N)$ traffic jams of centralized architectures.

- **Dissemination Barrier (`MPI_Barrier`):** $O(\log_2 N)$ synchronization using bitwise integer math.
- **Binomial Broadcast (`MPI_Bcast`):** Efficient data replication across a dynamically generated binomial tree.
- **Asynchronous Burst Scatter & Gather:** Leverages the `kqueue` engine to issue simultaneous non-blocking transfers for `MPI_Scatter` and `MPI_Gather`.
- **Compound Operations:** Flawless composition of primitives for `MPI_Allgather` and `MPI_Alltoall` (Distributed Matrix Transpose).
- **Inverse Reduction Tree (`MPI_Reduce` & `MPI_Allreduce`):** Distributed ALU computations (Sum, Max, Min, Prod) executed at every junction of the tree before propagating to the Root.

---

## Technical Stack

- **Language:** C (Standard C11)
- **OS/Hardware:** macOS (XNU/Darwin), Apple Silicon
- **Build System:** CMake + Ninja
- **System Primitives:** `kqueue`, `pthreads`, Unix Domain Sockets, POSIX Pipes/Signals

---

## Current Status

- [x] **Phase 1: Process Management** - Complete.
- [x] **Phase 2: Blocking Point-to-Point** - Complete.
- [x] **Phase 3: Background Progress Engine (`kqueue`)** - Complete.
- [x] **Phase 4: Foundational Collectives** - Complete.
- [x] **Phase 5: Compound Collectives & Reduction** - Complete.

---

## Roadmap & Future Work

While the library is highly functional, high-performance computing is an endless pursuit of lower latency. The following features represent the next generation of `macMPI`:

1. **POSIX Shared Memory (`mmap`) Transition:** Upgrading the intra-node transport layer. While Unix Domain Sockets are fast, true "Zero-Copy" requires mapping a shared POSIX memory file (`shm_open`) into the virtual memory space of all processes, completely bypassing the OS kernel for gigabyte-scale transfers.
2. **Derived Datatypes Engine:** Implementing `MPI_Type_commit` and `MPI_Type_vector` to allow users to send non-contiguous data blocks and complex C structs dynamically.
3. **Dynamic Sub-Groups (`MPI_Comm_split`):** Enabling the creation of isolated communicators dynamically at runtime to divide the computing cluster into specialized task forces.
4. **Performance Profiling:** Benchmarking latency and bandwidth against standard HPC metrics to document gigabytes-per-second (GB/s) throughput limits on M-Series unified memory.

---

## How to Build & Run

### Prerequisites

- Apple Clang (via Xcode Command Line Tools)
- CMake (>= 3.10)
- Ninja Build System

### Compilation

```bash
git clone [https://github.com/yourusername/macMPI.git](https://github.com/yourusername/macMPI.git)
cd macMPI
mkdir build && cd build
cmake -G Ninja ..
ninja
```
