/*
 * @file failure_detector.h
 * @author Shahzad Bhatti
 * @date Sep 28, 2016
 *
 */

#pragma once

#include "../logger/logger.h"

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

constexpr int MAXDATASIZE = 5000;
constexpr uint16_t PORT = 7777;
constexpr int NODES = 10;   // potential number of nodes in the system
constexpr int K = 3;        // number of nodes to ask for ping, see SWIM protocol paper


class sdfs;     // forward declaration

/*
 * This class detects failures of nodes in the system and keeps membership list updated
 * at all nodes. 
 * It can accept following command from the command line,
 * list - to show current list of nodes in the system,
 * id - birthtime of the node, which is used to uniquely identify a node.
 * join <number of another node> - this is used to join this node to the system via another node.
 * leave - to leave the network.
 *
 */

class failureDetector {

public:

/*
 * constructor for failureDetector object.
 * @param logFile name of the log the file.
 * @param number node number to get hostname.
 */        
failureDetector(int number, string logFile, level sLevel, sdfs* fs);

/*
 * This function is run in the main thread and is responsible for receiving and processing 
 * all the messages.
 */ 
void recvMessages();

/*
 * This function is responsible for pinging other nodes.
 * It is run in a separate thread.
 *
 */
void sendPING();

/*
 * process input from the command-line
 *
 */
void handleInput();

/*
 * send JOIN message to a node in the system to join the system.
 * @param otherNode node number to send join message.
 */
void sendJOIN(int otherNode);

/*
 * Leave the system.
 *
 */
void leave();

/*
 * get the birthTime of this node
 *
 */
uint64_t getBirthTime();

/*
 * print membership list
 *
 */
void printList();

/*
 * IPAddrs of all the potential nodes in the system
 *
 */
array<uint32_t, NODES+1> IPAddrs;


private:
/*
 * Create a UDP socket and bind it.
 * All sending and receiving is done through this socket.
 * 
 */
void createSocket();

/*
 * fill socket addresses for all other potential nodes in the system.
 * Addresses are stored in an array for fast access.
 *
 */
void fillAddrs();

/*
 * If a node does not send a direct ack (ACKD) in response to a ping message after timeout,
 * then request K other nodes to send pings to this node and reply if they receive an ack.
 * @param target number of node to send pings to.
 *
 */

void sendIndirectPINGS(int target);

/*
 * copy the id of the node consisting of birth time and ip address to the buf and update the
 * size variable.
 * @param buf char buffer to copy id.
 * @param size add to size the size of my id.
 *
 */
void copyMyID(char* buf, int &size);

/*
 * get number of a random node other than me.
 * @return number of the random node
 *
 */
int getRandomNode();

/*
 * get the number of node given its IP address.
 * @param IP IP address of the node.
 * @return number of the node.
 *
 */
int getNodeNumber(uint32_t IP);

/*
 * get IP address of host given its name.
 * @param hostname name of the node.
 * @return IP address of the node.
 *
 */
uint32_t getIP(const string &hostname);

/*
 * update sdfs of fialure of a node
 *
 */
void updateSdfs(int node);

/*
 * update sdfs of fialure of a node
 *
 */
void updateFileSystem(int node);

/*
 * addresses of other nodes
 *
 */
struct sockaddr_in nodeAddrs[NODES+1];

/*
 * birthTime of this node
 *
 */
uint64_t myBirthTime;

/*
 * IP address of this node
 *
 */
uint32_t myIP;

/*
 * number of this node
 *
 */
int myNumber;

/*
 * socket file descriptor
 *
 */
int sockFd;
/*
 * Indicator variable to tell sendJOIN method if a reply to JOIN message
 * has been received.
 *
 */
bool joinReply = false;

/*
 * Indicator array to check if an ACK has been received from a node.
 *
 */
array<bool, NODES+1> ackRecvd;

/*
 * membership list containing birth time and IP addresses of alive nodes.
 *
 */
map<uint64_t, uint32_t> list;

/*
 * instance of logger class to write logs to the logFile
 *
 */
logger log;

/*
 * instance of sdfs
 *
 */
sdfs* fileSystem;

};

