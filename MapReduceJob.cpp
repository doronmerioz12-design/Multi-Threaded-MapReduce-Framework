#include "MapReduceJob.h"
#include "MapContext.h"
#include "ReduceContext.h"
#include <algorithm>

/**
 * @brief Constructor of MapReduceJob.
 * Initializes the framework state, creates the worker threads, and starts the asynchronous execution.
 * * @param client Reference to the client object containing map and reduce implementations.
 * @param inputVec Vector containing the input key-value pairs to process.
 * @param multiThreadLevel Number of worker threads to spawn and utilize.
 */
MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
    : _client(client), _inputVec(inputVec), _multiThreadLevel(multiThreadLevel), _atomic_state(0)
{
    _intermediateVectors.resize(multiThreadLevel);
    _outputVectors.resize(multiThreadLevel);

    _barrier = std::make_unique<std::barrier<>>(multiThreadLevel);

    if (inputVec.empty()) {

        updateState(REDUCE_STAGE, 0, 0);
        _shuffleDone.store(true);
        _isOutputGenerated = true;

        return;
    }

    updateState(MAP_STAGE, inputVec.size(), 0);

    _threads.reserve(_multiThreadLevel);
    for (int i = 0; i < _multiThreadLevel; i++) {
        _threads.emplace_back(&MapReduceJob::worker, this, i);
    }
}

/**
 * @brief Thread-safe getter for the current job state.
 * Extracts stage, total, and processed components using precise bitwise operations.
 * * @return MapReduceState containing the quantized enum stage and completion percentage.
 */
MapReduceState MapReduceJob::getState(void) const
{
    // Atomic load prevents reading partially updated fields or corrupted states
    uint64_t current = _atomic_state.load();

    // De-serialize values using bitwise masks matching the 64-bit setup
    uint32_t stageVal = (current >> 62) & 0x3;
    uint32_t total = (current >> 31) & 0x7FFFFFFF;
    uint32_t processed = current & 0x7FFFFFFF;

    MapReduceState state;
    state.stage = static_cast<MapReduceStage>(stageVal);

    // Prevent edge-case Division-by-Zero errors
    if (total == 0) {
        state.percentage = 100.0;
    } else {
        state.percentage = (static_cast<double>(processed) / total) * 100.0;
        // Hard-cap at 100.0% to clean up potential floating-point precision drifts
        if (state.percentage > 100.0) state.percentage = 100.0;
    }
    return state;
}

/**
 * @brief Thread-safe blocking function that waits until all threads complete execution.
 * Guarantees no double-joining occurs.
 */
void MapReduceJob::wait(void)
{
    std::lock_guard<std::mutex> lock(_waitMutex); // Ensures cross-thread structural safety
    if (!_isJoined) {
        for (auto& t : _threads) {
            if (t.joinable()) {
                t.join(); // Joins the execution context safely
            }
        }
        _isJoined = true; // Flushed state flag to prevent re-joining
    }
}

/**
 * @brief Blocks until execution completes, then aggregates and sorts the final results.
 * * @return Sorted OutputVec containing all consolidated output pairs.
 */
OutputVec MapReduceJob::getOutput(void)
{
    wait(); // Ensure every working thread has completely halted execution
    std::lock_guard<std::mutex> lock(_outputMutex);

    // Lazily evaluate and aggregate the segmented output vectors exactly once
    if (!_isOutputGenerated) {
        for (int i = 0; i < _multiThreadLevel; i++) {
            _finalOutput.insert(_finalOutput.end(), _outputVectors[i].begin(),
                _outputVectors[i].end());
        }

        // Final mandatory lexicographical sort based on user custom keys operator<
        std::sort(_finalOutput.begin(), _finalOutput.end(),
                  [](const OutputPair& a, const OutputPair& b) {
                      return *(a.first) < *(b.first);
                  });
        _isOutputGenerated = true;
    }
    return _finalOutput;
}

/**
 * @brief Passive status inspector.
 * @return True if the job reached REDUCE_STAGE and completed all elements, false otherwise.
 */
bool MapReduceJob::isDone(void) const
{

    if (_isJoined) {
        return true;
    }

    MapReduceState state = getState();
    return (state.stage == REDUCE_STAGE && state.percentage == 100.0);
}

/**
 * @brief Blocking Destructor. Ensures no thread is left hanging, preventing memory leaks.
 */
MapReduceJob::~MapReduceJob()
{
    wait(); // Blocking destructor constraint fulfilled
}

/**
 * @brief Helper utility to safely re-write the global atomic layout configuration.
 */
void MapReduceJob::updateState(MapReduceStage stage, uint32_t total, uint32_t processed) {
    uint64_t stage64 = static_cast<uint64_t>(stage) << 62;
    uint64_t total64 = static_cast<uint64_t>(total) << 31;
    uint64_t processed64 = static_cast<uint64_t>(processed);
    _atomic_state.store(stage64 | total64 | processed64);
}

/**
 * @brief Atomic helper utility to increase the processed items field without affecting other bits.
 */
void MapReduceJob::advanceProcessed(uint32_t amount) {
    _atomic_state.fetch_add(amount);
}

/**
 * @brief Core multi-threaded lifecycle runner routine executed by each spawned thread.
 * Navigates from Map, Sort, Shuffle up to the parallelized Reduce phase pipeline.
 */
void MapReduceJob::worker(int threadId) {

    // =========================================================================
    // 1. MAP STAGE
    // =========================================================================
    MapContext mapContext(_intermediateVectors[threadId]); // Private vector buffer bound

    while (true) {
        // Dynamically grab a unique index safely without race-condition overlaps
        size_t currentIndex = _mapCounter.fetch_add(1);
        if (currentIndex >= _inputVec.size()) {
            break; // No more items left inside input stream
        }

        // Pass to client routine to populate the local intermediate vector bucket
        _client.map(_inputVec[currentIndex].first, _inputVec[currentIndex].second, mapContext);
        advanceProcessed(1); // Increment Map progress
    }

    // =========================================================================
    // 2. SORT STAGE
    // =========================================================================
    // Sort local vector by key so the Shuffle phase can peek the back sequentially
    std::sort(_intermediateVectors[threadId].begin(), _intermediateVectors[threadId].end(),
              [](const IntermediatePair& a, const IntermediatePair& b) {
                  return *(a.first) < *(b.first);
              });

    // Barrier Sync: Thread 0 must not start Shuffling until ALL sort phases end!
    _barrier->arrive_and_wait();

    // =========================================================================
    // 3. SHUFFLE STAGE (Thread 0 Only)
    // =========================================================================
    if (threadId == 0) {
        uint32_t totalIntermediate = 0;
        for (int i = 0; i < _multiThreadLevel; i++) {
            totalIntermediate += _intermediateVectors[i].size();
        }

        // Transition immediately into Shuffle stage status before notifying any threads
        updateState(SHUFFLE_STAGE, totalIntermediate, 0);

        while (true) {
            K2* maxKey = nullptr;

            // Look for the largest available key at the back of all sorted tracks
            for (int i = 0; i < _multiThreadLevel; i++) {
                if (!_intermediateVectors[i].empty()) {
                    K2* currentKey = _intermediateVectors[i].back().first.get();
                    if (maxKey == nullptr || (*maxKey < *currentKey)) {
                        maxKey = currentKey; // Discovered a higher maximum key bound
                    }
                }
            }

            // If maxKey remains nullptr, all elements across all intermediate vectors are exhausted
            if (maxKey == nullptr) break;

            // Gather all identical occurrences matching the maxKey criteria
            IntermediateVec newBatch;
            for (int i = 0; i < _multiThreadLevel; i++) {
                while (!_intermediateVectors[i].empty()) {
                    K2* currentKey = _intermediateVectors[i].back().first.get();
                    // Custom equivalence testing idiom using strictly operator<
                    if (!(*currentKey < *maxKey) && !(*maxKey < *currentKey)) {
                        newBatch.push_back(_intermediateVectors[i].back());
                        _intermediateVectors[i].pop_back(); // Pop from back efficiently
                        advanceProcessed(1); // Increment Shuffle progress counters
                    } else {
                        break; // Since vectors are sorted, encountering a different key terminates loop
                    }
                }
            }

            // Push the generated batch container safely into our thread-safe shared pipeline queue
            {
                std::lock_guard<std::mutex> lock(_queueMutex);
                _shuffledQueue.push_back(std::move(newBatch));
                _queueCV.notify_one();
            }
        }

        // Shuffle complete. Reset state layout to REDUCE_STAGE
        {
            std::lock_guard<std::mutex> lock(_queueMutex);
            updateState(REDUCE_STAGE, totalIntermediate, 0);
            _shuffleDone.store(true);
            _queueCV.notify_all();
        }
    }

    // =========================================================================
    // 4. REDUCE STAGE (Parallelized Pipeline)
    // =========================================================================
    ReduceContext reduceContext(_outputVectors[threadId]); // Output target registry

    while (true) {
        IntermediateVec batch;
        {
            std::unique_lock<std::mutex> lock(_queueMutex);

            // Lightweight conditional variable wait scheme to completely evade busy-waiting cycles
            _queueCV.wait(lock, [this] {
                return !_shuffledQueue.empty() || _shuffleDone.load();
            });

            // Termination condition: Queue empty and Shuffle finalized
            if (_shuffledQueue.empty() && _shuffleDone.load()) {
                break;
            }

            // Safely popping a consolidated vector from the back of the pipeline queue
            batch = std::move(_shuffledQueue.back());
            _shuffledQueue.pop_back();
        }

        // Execute the user client custom reduce operation outside of the mutex lock for max throughput!
        _client.reduce(batch, reduceContext);
        advanceProcessed(batch.size()); // Increment progress by the number of elements processed
    }
}