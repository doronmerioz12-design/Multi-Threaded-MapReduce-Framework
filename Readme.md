# Multi-Threaded MapReduce Framework

## Overview
This project is a robust, concurrent C++ implementation of the MapReduce programming model. It provides a multi-threaded framework designed to process and generate large datasets efficiently by distributing tasks across multiple CPU threads.

The framework abstracts complex synchronization, thread pool management, and state tracking from the user, allowing developers to focus solely on implementing their custom `Map` and `Reduce` logic.

## Architecture & Workflow
The MapReduce job executes in three strictly synchronized stages, managed autonomously by the framework:

1. **Map Stage:** Worker threads concurrently fetch input elements using atomic counters, execute the client's `map` function, and store intermediate `(K2, V2)` pairs in thread-local vectors.
2. **Shuffle Stage:** Thread 0 consolidates all thread-local intermediate vectors, sorts them, and groups identical keys into discrete batches (`IntermediateVec`), pushing them into a shared queue.
3. **Reduce Stage:** Worker threads concurrently pop key-batches from the shuffled queue, applying the client's `reduce` function to generate the final `(K3, V3)` pairs directly into thread-safe output partitions.

## Key Technical Features
* **Modern C++ Synchronization:** Utilizes C++20 `std::barrier` to enforce phase synchronization across worker threads without the overhead of heavy locking or condition variables.
* **Lock-Free State Tracking:** Implements a highly efficient 64-bit `std::atomic` tracker updated via low-level bitwise operations. This allows external clients to monitor job progression (Stage and Percentage) in real-time without blocking worker threads.
* **Thread-Safety & Race Avoidance:** Designed with localized memory partitions for intermediate and output stages, significantly reducing the need for global `std::mutex` locks during aggressive read/write cycles.

## Usage Example
To use the framework, inherit from the `MapReduceClient` class and override the `map` and `reduce` functions, then invoke the framework via the global controller function.

```cpp
#include "MapReduceFramework.h"
#include <iostream>

// 1. Define your custom Client
class MyClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, MapContext& context) const override {
        // Custom mapping logic
    }
    void reduce(const IntermediateVec* pairs, ReduceContext& context) const override {
        // Custom reducing logic
    }
};

int main() {
    MyClient client;
    InputVec inputData = { ... };
    int threadCount = 4;

    // 2. Start the Job using the official global entrypoint
    JobHandle handle = startMapReduceJob(client, inputData, threadCount);

    // 3. Monitor Progress (Optional)
    MapReduceState state;
    getJobState(handle, &state);
    while (state.stage != REDUCE_STAGE || state.percentage < 100.0) {
        std::cout << "Stage: " << state.stage << " | Progress: " << state.percentage << "%\n";
        getJobState(handle, &state);
    }

    // 4. Wait for completion and release resources
    waitForJob(handle);
    closeJobHandle(handle);
    
    return 0;
}