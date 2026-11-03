CXX      := c++
CXXFLAGS := -std=c++20 -Wall -Wextra -Wpedantic -O2
LDFLAGS  := -pthread
TARGET   := spsc_packets

.PHONY: all clean run

all: $(TARGET)

$(TARGET): main.cpp
	$(CXX) $(CXXFLAGS) $< $(LDFLAGS) -o $@

run: $(TARGET)
	./$(TARGET) 1000 5

clean:
	rm -f $(TARGET)
