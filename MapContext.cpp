#include "MapContext.h"

// implement here your constructor and destructor

MapContext::MapContext(IntermediateVec& vector)
     : _intermediateVec(vector)
{
}


MapContext::~MapContext()
{

}



void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{
    // TODO: implement this function

    _intermediateVec.push_back({key, value});

}
