# =============================================================================
# Operating Systems - HUJI
# Exercise 2: MapReduce Multi-threaded Framework Makefile
# =============================================================================

# Compiler and compilation flags
CC = g++
CFLAGS = -Wall -Wextra -std=c++20 -pthread

# Source files for the framework implementation
SRCS = MapReduceJob.cpp MapContext.cpp ReduceContext.cpp

# Generate object file names (.o) automatically from source file names (.cpp)
OBJS = $(SRCS:.cpp=.o)

# Target Static Library name requested by the exercise specifications
TARGET = libMapReduceFramework.a

# Default target executed when running 'make' without arguments
all: $(TARGET)

# Rule to create the static library archive using the object files
$(TARGET): $(OBJS)
	ar rcs $@ $^

# Pattern rule to compile each source (.cpp) file into an object (.o) file
%.o: %.cpp
	$(CC) $(CFLAGS) -c $< -o $@

# Clean rule to wipe temporary build artifacts and binaries before submission
clean:
	rm -f $(OBJS) $(TARGET)

# Declaring phony targets to avoid conflicts with files named 'all' or 'clean'
.PHONY: all clean