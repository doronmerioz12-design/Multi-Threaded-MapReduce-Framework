cmake_minimum_required(VERSION 3.10)

project(MapReduceFramework)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(FRAMEWORK_SOURCES
        MapReduceJob.cpp
        MapContext.cpp
        ReduceContext.cpp
        SentimentTest.cpp
        SentimentTest.cpp
)

add_library(mapreduce_framework STATIC ${FRAMEWORK_SOURCES})

find_package(Threads REQUIRED)

target_link_libraries(mapreduce_framework PRIVATE Threads::Threads)