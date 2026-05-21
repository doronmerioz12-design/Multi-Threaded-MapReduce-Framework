# Multi-Threaded MapReduce Framework

## Overview
This project is a robust, concurrent C++ implementation of the MapReduce programming model. It provides a multi-threaded framework designed to process and generate large datasets efficiently by distributing tasks across multiple CPU threads.

The framework abstracts the complex synchronization, thread pool management, and state tracking from the user, allowing developers to focus solely on implementing their custom `Map` and `Reduce` logic.

## Architecture & Workflow
The MapReduce job executes in four strictly synchronized stages, managed autonomously by the framework:

1. **Map Stage (`doMap`):** Each thread safely fetches an input pair and executes the client's map function, generating intermediate `(K2, V2)` pairs stored in thread-local vectors.
2. **Shuffle Stage (`doShuffle`):** Thread 0 takes control, gathering all intermediate pairs from all threads, and organizing them into discrete batches grouped and sorted by identical keys.
3. **Reduce Stage (`doReduce`):** Worker threads concurrently pull key-batches from the shuffled queue, applying the client's reduce function to generate the final `(K3, V3)` pairs.
4. **Merge Stage (`doMerge`):** Thread 0 merges all localized output vectors into a single, cohesive, and sorted final output vector.

## Key Technical Features
* **Advanced Concurrency Primitives:** Utilizes C++20 `std::barrier` to enforce phase synchronization without the overhead of heavy locking or condition variables.
* **Lock-Free State Tracking:** Implements a highly efficient 64-bit `std::atomic` tracker updated via low-level bitwise operations. This allows external clients to monitor job progression (Stage and Percentage) in real-time without blocking worker threads.
* **Thread-Safety & Race Avoidance:** Designed with localized output vectors for each thread (`_intermediateVectors[threadId]`, `_outputVectors[threadId]`), significantly reducing the need for `std::mutex` locks during aggressive read/write cycles.

## Usage Example
To use the framework, inherit from the `MapReduceClient` class and override the `map` and `reduce` functions.

```cpp
#include "MapReduceJob.h"

// 1. Define your custom Client
class MyClient : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext &context) const override { ... }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override { ... }
};

int main() {
    MyClient client;
    InputVec inputData = { ... };
    int threadCount = 4;

    // 2. Start the Job
    MapReduceJob job(client, inputData, threadCount);

    // 3. Monitor Progress (Optional)
    while(!job.isDone()) {
        MapReduceState state = job.getState();
        std::cout << "Stage: " << state.stage << " | Completed: " << state.percentage << "%\n";
    }

    // 4. Retrieve Results
    OutputVec results = job.getOutput();
    return 0;
}