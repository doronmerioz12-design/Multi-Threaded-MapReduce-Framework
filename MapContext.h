#ifndef MAP_CONTEXT_H
#define MAP_CONTEXT_H

class MapContext;

#include "MapReduceKeys.h"
#include "MapReduceClient.h"



class MapContext
{
public:
    MapContext(IntermediateVec& vector);
    ~MapContext();

    /*
    You must keep and implement this function:
    */
    void addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value);

private:
    IntermediateVec& _intermediateVec;
};

#endif // MAP_CONTEXT_H