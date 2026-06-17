# macMPI: High-Performance MPI-1.1 Implementation for macOS Architecture

[![C Standard](https://img.shields.io/badge/Language-C11-blue.svg)](https://en.wikipedia.org/wiki/C11_(C_standard_revision))
[![Platform Support](https://img.shields.io/badge/Platform-macOS%20%28M--Series%29-black.svg)](https://developer.apple.com/apple-silicon/)
[![Build System](https://img.shields.io/badge/Build-CMake%20%2B%20Ninja-success.svg)](https://ninja-build.org/)

**macMPI** is a low-latency, single-node implementation of the MPI-1.1 specification, engineered specifically for XNU/Darwin kernels and optimized for Apple Silicon (M-Series unified memory architectures). Designed to operate without high-level runtime reliance, `macMPI` handles process coordination, message routing, and event multiplexing using bare-metal POSIX and macOS-native kernel primitives.

The library features a decentralized process manager, a full-mesh IPC topology over Unix Domain Sockets, and a highly concurrent, multi-threaded progress engine driven by native `kqueue` event notifications to maximize compute-communication overlap.

---

## Architectural Implementation Details

### 1. Decentralized Process Management (`mpirun`)

The execution environment is driven by a custom runtime daemon responsible for process lifecycle orchestration, virtual topology construction, and terminal I/O streaming.

* **Runtime Environment Injection:** Implements robust process separation via `fork()` and `execvp()` boundaries. The daemon safely maps topological metadata (e.g., `MPI_RANK`, `MPI_UNIVERSE_SIZE`) and abstract communication maps directly into target address spaces before payload initialization.
* **Non-Blocking Stream Multiplexing:** Completely isolates child standard output (`stdout`/`stderr`) using unidirectional POSIX pipes (`pipe()`, `dup2()`). Reads are non-blocking and multiplexed through an event-driven `poll()` daemon loop, eliminating Head-of-Line (HoL) blocking across standard streams.
* **Asynchronous Lifecycle Control:** Employs precise signal management (`SIGINT`, `SIGCHLD`) to coordinate graceful cascading tear-downs, ensuring all isolated process trees are collected without producing orphaned or zombie processes.

### 2. Point-to-Point Messaging Layer

The point-to-point layer establishes an explicit communication fabric using a full-duplex Unix Domain Socket mesh.

* **Fixed-Overhead Routing Envelopes:** Every communication transaction prefixes a standardized 64-byte metadata header containing explicit source, target, tag, and payload bounds to assure deterministic matching.
* **Kernel-Safe Flow Regulation:** Points of egress implement strict loop-driven buffer adjustments to mitigate partial system writes (`write()`), preventing deadlocks arising from XNU kernel socket buffer constraints during saturated transmission bursts.
* **Unexpected Message Buffering (UMQ):** Full compliance with `MPI_ANY_SOURCE` and `MPI_ANY_TAG` wildcards. Messages received out-of-order are extracted immediately from kernel buffers and queued into a heap-allocated Unexpected Message Queue (UMQ) to protect network resources.

### 3. Asynchronous Progress Engine

To achieve deterministic compute-communication overlap, `macMPI` decouples user execution flows from the transport medium via an independent background execution subsystem.

* **Hardware Core Affirmation:** Spawns a dedicated POSIX shadow thread (`pthread`) explicitly bound to macOS Performance Cores utilizing Apple-specific Quality of Service APIs (`pthread_set_qos_class_self_np`).
* **$O(1)$ Event Despatching:** Leverages macOS kernel event queues (`kqueue`/`kevent`) with `EVFILT_READ` configurations. This circumvents linear polling limits, enabling zero-CPU-overhead socket state observation across dense rank meshes.
* **Thread-Safe Concurrency Architecture:** Implements non-blocking APIs (`MPI_Isend`, `MPI_Irecv`, `MPI_Wait`, `MPI_Test`) using a highly disciplined synchronization model managed via mutual exclusions (`pthread_mutex_t`) and condition primitives (`pthread_cond_t`).

### 4. Optimized Collective Suites

`macMPI` bypasses centralized "star" topologies to protect the Root rank from structural bottlenecks, routing collective operations through decentralized data-movement paths instead.

* **Dissemination Barrier (`MPI_Barrier`):** Implements synchronization spanning an $O(\log_2 N)$ communications curve driven by bitwise index shifts.
* **Binomial Tree Broadcast (`MPI_Bcast`):** Replicates algorithmic data arrays through dynamically instantiated binomial tree topologies, ensuring uniform bandwidth allocation.
* **Asynchronous Slice Allocation (`MPI_Scatter` / `MPI_Gather`):** Leverages pointer arithmetic and non-blocking engines to execute synchronous matrix slice dispersal and ordered array collections across generic memory layouts.
* **Compound Collective Topology:** Builds clean behavioral composition layers for global distribution operations, including `MPI_Allgather` and distributed matrix transpositions via `MPI_Alltoall`.
* **Inverse Tree Reductions (`MPI_Reduce` / `MPI_Allreduce`):** Distributes Arithmetic Logic Unit (ALU) evaluations (such as `MPI_SUM`, `MPI_MAX`, `MPI_MIN`, `MPI_PROD`) iteratively down structural nodes prior to Root delivery.

---

## Technical Architecture Stack

* **Core Dialect:** ISO C11 (Strict compliance, zero third-party framework runtime dependencies).
* **Operating Systems:** macOS 12.0+ (Darwin/XNU Kernel architecture).
* **Target Architectures:** Apple Silicon ARM64 (M-Series Execution Optimizations).
* **Prerequisites:** Apple Clang Compiler, CMake (>= 3.10), Ninja Build System.

---

## Project Milestones

- [x] **Phase 1: Process Management & Lifecycle Daemon** — *Complete*
- [x] **Phase 2: Synchronous Point-to-Point Fabric** — *Complete*
- [x] **Phase 3: Multi-threaded Progress Subsystem (`kqueue` Integration)** — *Complete*
- [x] **Phase 4: Foundational Distributed Collective Primitives** — *Complete*
- [x] **Phase 5: Matrix Transform Operations & Reduction Suite** — *Complete*

---

## Architectural Optimization Roadmap

1.  **POSIX Shared Memory (`mmap`) Migration:** Transitioning internal intra-node data transactions to memory-mapped POSIX files (`shm_open`). This design enhancement completely bypasses XNU kernel socket allocations for gigabyte-scale transfers, executing true zero-copy shared memory access.
2.  **Dynamic Strided Datatype Subsystem:** Integrating abstract datatype compilation engines (`MPI_Type_commit`, `MPI_Type_vector`) to serialize and transmit non-contiguous memory structures and complex layouts.
3.  **Dynamic Topology Partitioning (`MPI_Comm_split`):** Implementing runtime sub-group derivation to allow independent execution groups to compute along decoupled sub-communicator paths.
4.  **HPC Metrics Validation:** Formal automated latency and bandwidth benchmarking mapping performance profiles directly against established unified-memory models.

---

## Installation & Execution

### Build System Initialization

Compile the library and runtime tools using CMake and the Ninja build manager:

```bash
# Clone the repository
git clone [https://github.com/yourusername/macMPI.git](https://github.com/yourusername/macMPI.git)
cd macMPI

# Configure and compile binary layers
mkdir build && cd build
cmake -G Ninja ..
ninja
