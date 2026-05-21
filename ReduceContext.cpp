class ReduceContext;

#include "ReduceContext.h"



ReduceContext::ReduceContext(OutputVec& vector)
    : _outputVec(vector)
{
}

ReduceContext::~ReduceContext()
{
}

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    _outputVec.push_back({key, value});
}