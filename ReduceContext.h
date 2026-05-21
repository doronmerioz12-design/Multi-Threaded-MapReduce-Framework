#ifndef REDUCE_CONTEXT_H
#define REDUCE_CONTEXT_H

#include "MapReduceKeys.h"
#include "MapReduceClient.h"

class ReduceContext
{
public:
    ReduceContext(OutputVec& vector);
    ~ReduceContext();

    void addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value);

private:
    OutputVec& _outputVec;
};

#endif // REDUCE_CONTEXT_H