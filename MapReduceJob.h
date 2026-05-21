#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <barrier>

enum MapReduceStage
{
    UNDEFINED_STAGE, // 0
    MAP_STAGE, // 1
    SHUFFLE_STAGE, // 2
    REDUCE_STAGE // 3
};

class MapReduceState
{
public:
    MapReduceStage stage;
    double percentage;

    inline bool operator==(const MapReduceState &other) const
    {
       return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
    }

    inline bool operator!=(const MapReduceState &other) const
    {
       return !(*this == other);
    }
};

class MapReduceJob
{
public:
    /*
    You CAN NOT change or add properties to this part (public API).
    */

    MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

    ~MapReduceJob();

    MapReduceState getState(void) const;

    bool isDone(void) const;

    void wait(void);

    OutputVec getOutput(void);

private:
    /*
    -------------------------------------------------------------------------
    Private Framework Components & Synchronization Primitives
    -------------------------------------------------------------------------
    */
    const MapReduceClient& _client;
    const InputVec& _inputVec;
    int _multiThreadLevel;

    // 64-bit atomic state variable containing: Stage (2 bits), Total (31 bits), Processed (31 bits)
    std::atomic<uint64_t> _atomic_state;

    // Thread management arrays
    std::vector<std::thread> _threads;
    std::vector<IntermediateVec> _intermediateVectors;
    std::vector<OutputVec> _outputVectors;

    // Shared thread-safe pipeline queue for the Shuffle -> Reduce sequence
    std::vector<IntermediateVec> _shuffledQueue;
    std::mutex _queueMutex;
    std::condition_variable _queueCV;
    std::atomic<bool> _shuffleDone{false};

    // Consolidated final result buffers and safety locks
    OutputVec _finalOutput;
    std::mutex _outputMutex;
    bool _isOutputGenerated{false};

    // Structural guard for the blocking wait() API
    std::mutex _waitMutex;
    bool _isJoined{false};

    // Atomic tracker for dynamic work-stealing during the Map phase
    std::atomic<size_t> _mapCounter{0};

    // C++20 Barrier to cleanly orchestrate barrier synchronization across phases
    std::unique_ptr<std::barrier<>> _barrier;

    // Internal worker thread lifecycle management routines
    void worker(int threadId);
    void updateState(MapReduceStage stage, uint32_t total, uint32_t processed);
    void advanceProcessed(uint32_t amount);
};

#endif // MAP_REDUCE_JOB_H