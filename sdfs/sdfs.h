/*
 * @file sdfs.h
 * @date Oct 24, 2016
 *
 */

#pragma once

#include "../logger/logger.h"
#include "../failure_detector/failure_detector.h"

#include <algorithm>
#include <array>
#include <arpa/inet.h>
#include <cstring>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <netdb.h>
#include <string>
#include <stdlib.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace std;

constexpr int MAXDATASIZE2 = 5000;
constexpr uint16_t PORT2 = 6666;

class failureDetector;  // forward declaration


/*
 * This class implements a Simple Distributed File System (SDFS). Data stored in sdfs is tolerant
 * to failures of two machines at a time. Following operations are supported.
 * 1. adding a file to sdfs.
 * 2. fetching a file from sdfs.
 * 3. deleting a file from sdfs.
 * 4. list machines storing copies of a file.
 * 5. list all files stored in current machine.
 *
 */

class sdfs {

public:

/*
 * constructor for sdfs object.
 * @param logFile name of the log file.
 * @param number node number to get hostname.
 */
sdfs(int number, string logFile, level sLevel);

/*
 * This function is responsible for receiving and processing
 * all the messages.
 */
void recvMessages();

/*
 * Store a local file in sdfs.
 * @param localName name of the file to be stored
 * @param sdfsName name of the file in sdfs
 *
 */
void storeFile(string localName, string sdfsName);

/*
 * Store a local file in sdfs.
 * @param sdfsName name of the file stored in sdfs
 * @param localName name of the file in local directory
 *
 */
void fetchFile(string sdfsName, string localName);

/*
 * Delete a file in sdfs.
 * @param filename of the file stored in sdfs
 *
 */
void deleteFile(string filename);

/*
 * remove file if it is stored at this node.
 * @param filename of the file stored in sdfs 
 *
 */
void removeFile(string filename);

/*
 * update file distribution given the new successor
 *
 */
void updateFileDistribution(char fileLabel);

/*
 * process input from the command-line
 *
 */
void handleInput();

/*
 * show all the files stored at this node
 *
 */
void showStore();

/*
 * show all the nodes storing copies of a file
 *
 */
void showFileLocations(string fileName);

/*
 *
 * new Node joined
 *
 */
void newNode(int node);

/*
 *
 * node failed
 *
 */
void nodeFailure(int node);

/*
 * instance of failureDetector
 *
 */
failureDetector* fd;

/*
 * current nodes in the system
 *
 */
array<bool, NODES+1> ring;

private:
/*
 * Create a UDP socket and bind it.
 * All sending and receiving is done through this socket.
 *
 */
void createSocket();

/*
 * get the number of node given its IP address.
 * @param IP IP address of the node.
 * @return number of the node.
 *
 */
int getNodeNumber(uint32_t IP);

/*
 * replicate a file on successor
 * @param fileName
 *
 */
void sendFileToSuccessor(string fileName, char label);

/*
 * send file to a node
 * @param node target VM number to send file
 * @fileName file to send
 *
 */
void sendFile(int requestNode, string localName, string sdfsName, char label);

/*
 * receive file from a buf
 *
 */
string recvFile(char * recvBuf, int connFd, int numBytes, int offset);

/*
 *
 * send file to targetNode to store in sdfs
 *
 */

bool pushFileToNode(int targetNode, string localFile, string remoteFile, string node);

/*
 * send a message to delete a file
 *
 */
void sendDeleteMessage(int node, string fileName);

/*
 * send a message to get a file
 *
 */
void sendGetMessage(int requestNode, int hostNode, string sdfsName, string localName, char label);

/*
 * send exist message in response to query message
 *
 */
void sendExistMessage(int node, char label);

/*
 * send Query message to check if a file exits
 *
 */
void sendQueryMessage(int requestNode, int hostNode, string fileName);

/*
 * update file distribution after a node failure
 *
 */
void updateFileDistribution();


/*
 * create UPDA messages containing updated file type and filenames
 *
 */
int createUpdaMsg(char* msg, vector<string>& filenames, char fileType);

/*
 * send messages to check distribution correctness of mastering files (fileA)
 *
 */
void requestUpdateMasteringFiles();
/*
 * connect to server at targetNode
 *
 */

int connectToServer(int targetNode, int *connectionFd);

/*
 * print the current ring
 *
 */
void printRing();


/*
 * successor of a node
 *
 */
int successorNode(int node);

/*
 * predecessor of a node
 *
 */
int predecessorNode(int node);


/*
 * location of the file
 * @param filename of the file
 * @return number of node for the file based on hash function
 *
 */
int location(const string &filename);

/*
 * hash all files to update the ids, call when membership list changes
 *
 */
void updateFileIds();
/*
 *
 * my VM Number
 *
 */
int myNumber;

/*
 * socket file descriptor
 *
 */
int sockFd;

/*
 * files (and their label) stored at this machine.
 *
 */
map<string, char> files;

/*
 * missing files (and their label) after a node failure
 *
 */
map<string, char> missingFiles;

/*
 * instance of logger class to write logs to the logFile
 *
 */
logger log;

/*
 * indicator for update thread
 *
 */
mutex updateFileDistMutex;
};

