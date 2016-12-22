##
# Makefile
#
# Simple Makefile to build program
#

CXX = g++
CXXFLAGS = -Iinclude -std=c++1y -g -O0 -c -Wall -Wextra -Wl,--no-as-needed -lpthread
LDFLAGS = -Wl,--no-as-needed -lpthread -std=c++11

.PHONY: all clean tidy

ifdef SANITIZE
CXXFLAGS += -fsanitize=$(SANITIZE)
LDFLAGS += -fsanitize=$(SANITIZE)
endif

EXENAME = query-log send-log node
OBJECTS = log_querier.o log_sender.o logger.o failure_detector.o sdfs.o mapleJuice.o util.o node.o

all : $(EXENAME)

query-log : log_querier.o
	$(CXX) log_querier.o $(LDFLAGS) -o query-log

send-log : log_sender.o
	$(CXX) log_sender.o $(LDFLAGS) -o send-log

log_querier.o : grep/log_querier.cc
	$(CXX) $(CXXFLAGS)  grep/log_querier.cc

log_sender.o : grep/log_sender.cc
	$(CXX) $(CXXFLAGS)  grep/log_sender.cc

node : node.o logger.o failure_detector.o sdfs.o mapleJuice.o
	$(CXX) node.o logger.o failure_detector.o util.o sdfs.o mapleJuice.o $(LDFLAGS) -o node

node.o : node.cc logger.o failure_detector.o sdfs.o mapleJuice.o
	$(CXX) node.cc $(CXXFLAGS)
   
mapleJuice.o : mapleJuice/mapleJuice.cc logger.o util.o failure_detector.o sdfs.o
	$(CXX) $(CXXFLAGS) mapleJuice/mapleJuice.cc

failure_detector.o : failure_detector/failure_detector.cc logger.o util.o sdfs.o
	$(CXX) $(CXXFLAGS) failure_detector/failure_detector.cc

sdfs.o : sdfs/sdfs.cc logger.o util.o failure_detector.o
	$(CXX) $(CXXFLAGS) sdfs/sdfs.cc

logger.o : logger/logger.cc
	$(CXX) $(CXXFLAGS) logger/logger.cc

util.o : util/util.cc
	$(CXX) $(CXXFLAGS) util/util.cc

doc: $ distributed.doxygen
	doxygen distributed.doxygen

clean:
	rm -f $(EXENAME) *.o 2>/dev/null

tidy:
	rm -rf doc
