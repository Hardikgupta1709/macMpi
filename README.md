# macMPI: High-Performance MPI-1.1 Implementation for Apple Silicon

[![Language](https://img.shields.io/badge/Language-C11-blue.svg)](<https://en.wikipedia.org/wiki/C11_(C_standard_revision)>)
[![Platform](https://img.shields.io/badge/Platform-macOS%20%28M--Series%29-black.svg)](https://developer.apple.com/apple-silicon/)
[![Build](https://img.shields.io/badge/Build-CMake%20%2B%20Ninja-success.svg)](https://ninja-build.org/)

**macMPI** is a low-latency, single-node implementation of the MPI-1.1 standard built entirely from scratch in C11. Engineered specifically for the XNU/Darwin kernel and optimized for Apple Silicon (M-Series) Unified Memory, the library bypasses standard OS network stacks to deliver high-throughput communication.

By combining a native macOS `kqueue` control plane with a POSIX shared-memory (`mmap`) data plane, macMPI reaches **102.99 GB/s of point-to-point bandwidth** in the benchmark shown below.

---

## Architectural Subsystems

### 1. Hybrid Transport Layer

Standard socket-based IPC can be limited by kernel buffering and context switching. macMPI addresses this with a split architecture:

- **Data plane (`mmap` / `shm_open`):** Ranks allocate and partition a shared-memory arena into isolated, lock-free 64 MB slab ring buffers. Payload transmission is reduced to a direct `memcpy()` operating at memory speed.
- **Control plane (Unix domain sockets):** Sockets carry only fixed-overhead, 64-byte signaling headers. They act as asynchronous doorbells that notify receivers of memory offsets, reducing head-of-line blocking and kernel congestion.

### 2. Non-Blocking Progress Engine

User execution is decoupled from the transport layer through a dedicated background subsystem to support compute-communication overlap.

- **Performance-core affinity:** A dedicated POSIX shadow thread (`pthread`) uses Apple-specific QoS APIs such as `pthread_set_qos_class_self_np`.
- **O(1) event dispatching:** Native macOS event queues (`kqueue` / `kevent`) monitor peer sockets without CPU polling.
- **Two-way matching and UMQ:** Fully asynchronous `MPI_Isend` and `MPI_Irecv` operations are supported. Out-of-order messages are moved to an Unexpected Message Queue (UMQ) until a matching receive is posted.

### 3. Decentralized Process Management (`mpirun`)

A standalone daemon manages process lifecycles, virtual topology construction, and terminal I/O streaming.

- **Environment injection:** Process separation is implemented through `fork()` / `execvp()` boundaries, with topological metadata injected into the target address spaces.
- **Non-blocking stream multiplexing:** Child `stdout` and `stderr` streams are isolated with POSIX pipes and multiplexed through an event-driven `poll()` loop.

### 4. Advanced Collective Operations

Collective operations use decentralized communication structures to avoid bottlenecking the root rank:

- **Dissemination barrier (`MPI_Barrier`):** `O(log₂ N)` synchronization driven by bitwise index shifts.
- **Binomial-tree broadcast (`MPI_Bcast`):** Algorithmic data replication for more uniform bandwidth allocation.
- **Asynchronous slice allocation (`MPI_Scatter` / `MPI_Gather`):** Pointer arithmetic and non-blocking operations distribute matrix slices and collect ordered arrays across generic memory layouts.
- **Compound operations (`MPI_Allgather` / `MPI_Alltoall`):** Parallel non-blocking operations support data replication and distributed matrix transpositions.
- **Inverse-tree reductions (`MPI_Reduce` / `MPI_Allreduce`):** Arithmetic operations such as `MPI_SUM` and `MPI_MAX` are evaluated iteratively up the tree before results are propagated from the root.

---

## Performance & Benchmarks

The following terminal output shows a 19.07 MB shared-memory payload benchmark executed over 1,000 iterations:

![macMPI shared-memory benchmark terminal output](assets/macmpi-shared-memory-benchmark.png)

---

## Project Status & Roadmap

- [x] Phase 1: Process Management & Lifecycle Daemon
- [x] Phase 2: Point-to-Point Messaging Fabric
- [x] Phase 3: Multi-threaded Progress Engine (`kqueue`)
- [x] Phase 4: Foundational Collective Primitives
- [x] Phase 5: POSIX Shared Memory (`mmap`) Migration
- [x] Phase 6: HPC Shared-Memory Benchmarking

### Ongoing / Future Work

- **Derived Datatypes Engine:** Implement `MPI_Type_commit` and `MPI_Type_vector` to serialize and unpack non-contiguous memory structures such as 2D matrix columns and complex C structs.
- **Dynamic Topology Partitioning (`MPI_Comm_split`):** Create isolated communicators at runtime to divide the compute environment into specialized task groups.

---

## Compilation & Execution

### Prerequisites

- Apple Clang, available through the Xcode Command Line Tools
- CMake 3.10 or newer
- Ninja

### Build Instructions

```bash
# Clone the repository
git clone https://github.com/hardikgupta1709/macMPI.git
cd macMPI

# Configure and build
cmake -S . -B build -G Ninja
cmake --build build
```

### Run the Benchmark

From the repository root:

```bash
./build/mpirun -n 8 "$(pwd)/tests/benchmark_shm"
```
