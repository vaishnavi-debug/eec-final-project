# Compiler
CXX = g++
# Compiler flags
CXXFLAGS = -Wall -std=c++17
# Include directories
INCLUDES = -I.

# Only Scheduler.cpp is student-written; the rest are pre-compiled framework objects
SCHEDULER_OBJ = Scheduler.o

FRAMEWORK_OBJ = Init.o Machine.o main.o Simulator.o Task.o VM.o

# Executable
TARGET = simulator

# Default target
all: $(TARGET)

$(TARGET): $(SCHEDULER_OBJ) $(FRAMEWORK_OBJ)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $(TARGET) $(SCHEDULER_OBJ) $(FRAMEWORK_OBJ)

scheduler: $(SCHEDULER_OBJ) $(FRAMEWORK_OBJ)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o scheduler $(SCHEDULER_OBJ) $(FRAMEWORK_OBJ)

# Compile only the student-written scheduler
$(SCHEDULER_OBJ): Scheduler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c Scheduler.cpp -o $(SCHEDULER_OBJ)

# Only clean student-built files; framework .o files must stay
clean:
	rm -f $(SCHEDULER_OBJ) $(TARGET) scheduler
