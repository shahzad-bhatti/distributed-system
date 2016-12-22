
/*
 * @file mapleJuice.h
 * @date Nov 29, 2016
 *
 */

#pragma once

#include "../logger/logger.h"
#include "../sdfs/sdfs.h"

#include <algorithm>
#include <array>
#include <arpa/inet.h>
#include <condition_variable>
#include <cstring>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <netdb.h>
#include <queue>
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
#include <unordered_map>
#include <unordered_set>

using namespace std;

constexpr int MAXDATASIZE3 = 50000;
// avoid redefination
// constexpr uint16_t PORT3 = 5555;


/*
 * This class implements a Simple version of MapReduce
 * Map phase is started as
 * maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
 * Reduce phase is started as
 * juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
 *
 */

struct maple {
    string mapleExe;
    int numMaples;
    string sdfsIntermediateFileNamePrefix;
    string sdfsSrcDirectory;
};

struct juice {
    string juiceExe;
    int numJuices;
    string sdfsIntermediateFileNamePrefix;
    string sdfsDestFileName;
    bool deleteInput;
};

struct juiceJob {
    string juiceExe;
    int numJuices;
    string sdfsIntermediateFileNamePrefix;
    int juicerIndex; //0-based
    string juicerInputFile;
    string sdfsDestDirectory;
    bool deleteInput;
};

class mapleJuice {

public:

/*
 * constructor for MapleJuice object.
 * @param singleton logger to log message.
 * @param number node number to get hostname.
 */
mapleJuice(int number, logger &logg);

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
 * process input from the command-line
 *
 */
void handleInput();

/*
 *
 * run juice job
 *
 */
void runJuiceJob(juice j, int myJuiceNumber ,int senderNode);

private:
/*
 * Create a UDP socket and bind it.
 * All sending and receiving is done through this socket.
 *
 */
void createSocket();

/*
 *
 * master function
 *
 */
void master();


/*
 *
 * send maple jobs to workers
 *
 */
void sendMapleJobs(const maple& m);
void sendMapleJobForFailNode(const maple &m, int fialNode);

/*
 *
 * handle Maple Job
 *
 */
void handleMapleJob(char* buf, int node);

/*
 *
 * run maple job
 *
 */
void runMapleJob(maple m, int node);

/*
 * store maple out files in sdfs
 *
 */
void storeMapleOutFiles(unordered_set<string>& deleteThese);

/*
 * store juice out files in sdfs
 *
 */
void storeJuiceOutFile(string fileName);


/*
 * send maple job done message
 *
 */
void sendMapleDoneMessage(int node);

/*
 * send juice job done message
 *
 */
void sendJuiceDoneMessage(int node);

/*
 *
 * handle when a maple job done message is received.
 *
 */
void handleMapleJobDone(int node);

//MJ
void handleFailure(int failNode);

/*
 * condition variable to notify that all maple jobs are done
 *
 */
condition_variable cvMapleJobDone;
mutex cvMapleJobDoneMutex;

/*
 *
 * indicator to check if all maple jobs are done
 *
 */
bool isAllMapleJobsDone;

/*
 * condition variable to notify that a juice task is issued
 *
 */
condition_variable cvWaitingForJuiceTask;
mutex cvWaitingForJuiceTaskMutex;

/*
 *
 * indicator to check if a juice job has been issued
 *
 */
bool isJuiceTaskIssued;

/*
 * condition variable to notify that all juice jobs are done
 *
 */
condition_variable cvJuiceJobDone;
mutex cvJuiceJobDoneMutex;

/*
 *
 * indicator to check if all maple jobs are done
 *
 */
bool isAllJuiceJobsDone;

/*
 *
 * process all the files received from juices
 *
 */
void collectJuiceFiles(string fileName, int count);

/*
 *
 * juicer nodes with their ids.
 *
 */
unordered_map<int, int> juiceIDs;

/*
 *
 * send juice job to workers
 *
 */
void sendJuiceJobs(const juice& j);

/*
 *
 * handle Juice Job
 *
 */
void handleJuiceJob(char* buf, int node);

int getFreeNode();
/*
 *
 * handle finished Juice Job, write as final output
 *
 */
void handleJuiceJobDone(int juicerNode);

/*
 * hash partition for shuffler
 *
*/
void partition(juiceJob& jb, string& sdfs_juice_input_filename_prefix);

/*
 * connect to server at targetNode
 *
 */
int connectToServer(int targetNode, int *connectionFd);


bool isNodeFree(int nodeNum);
/*
 * socket file descriptor
 *
 */
int sockFd;

/*
 * master node indicator
 *
 */
bool isMaster;

mutex isMasterMutex;

/*
 * instance of sdfs
 *
 */
sdfs fs;

/*
 * instance of logger class to write logs to the logFile
 *
 */
logger& log;

/*
 * queue of maple commands
 *
 */
queue<maple> mapleQ;

/*
 * queue of juice commands
 *
 */
queue<juice> juiceQ;


/*
 * record of sent juice Jobs, index to juiceJob, index is node number, len of N
 * master store one dummy job on itself's slot, be the last one to remove
 */
unordered_map<int, juiceJob> juiceJobSent;

/*
 *
 * range of files Allotted to each worker
 *
 */
unordered_map<int, pair<string, string>> filesAllottedForMaple;

/*
 *
 * number alloted to a juicer
 *
 */
unordered_map<int, int> juicerNumber;

/*
 *
 * input fileNames for maple phase
 *
 */
unordered_set<string> mapleFiles;

/*
 *
 * output fileNames for maple phase
 *
 */
unordered_set<string> mapleOutFiles;







};
