#include <arpa/inet.h>
#include <cstring>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <netdb.h>
#include <string>
#include <stdlib.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

constexpr int MAXDATASIZE = 5000;
constexpr const char* PORT = "5900";

using namespace std;


void communicate(int threadNo, string sendBuf ) {
    int sockFd, numBytes, rv;
    struct addrinfo hints, *servInfo, *p;
    char recvBuf[MAXDATASIZE], str[INET_ADDRSTRLEN];

    // construct host name
    string hostName = "fa16-cs425-g27-";
    if (threadNo < 10) {
        hostName += "0";
        hostName += to_string(threadNo);
    } else {
        hostName += to_string(threadNo); 
    }
    hostName += ".cs.illinois.edu";
    
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM; 
    if ((rv = getaddrinfo(hostName.data(), PORT, &hints, &servInfo)) != 0) {
        cerr << "socket: " << strerror(errno) <<  gai_strerror(rv) << endl;
        exit(EXIT_FAILURE);
    }

    // loop through all the results and connect to the first we can
    for(p = servInfo; p != NULL; p = p->ai_next) {
        if ((sockFd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            cerr << "socket: " << strerror(errno) << endl;
            continue;
        }

        if (connect(sockFd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockFd);
            continue;
        }
        break;
    }
    // if host is not up
    if (!p) {
    //    cerr << "Failed to connect to " << hostName << endl;
        return;
    }
    // get ip address of the host
    inet_ntop(p->ai_family, &(((struct sockaddr_in *)p->ai_addr)->sin_addr), str, INET_ADDRSTRLEN);
    // cout << "connecting to " << hostName << " at " << str << endl;
    freeaddrinfo(servInfo); // all done with this structure
    
    // send query to the host 
    if (send(sockFd, sendBuf.data(), sendBuf.size(), 0) == -1) {
        cerr << "send: " << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }
    
    // receive results of grep command
    size_t bytesToRead = 0, bytesRead = 0;
    numBytes = recv(sockFd, &bytesToRead, sizeof(size_t), 0);
    string recvString = "machine." + to_string(threadNo) + ":\n";
    
    while(bytesRead != bytesToRead) {
        if ((numBytes = recv(sockFd, recvBuf, MAXDATASIZE-1, 0)) > 0) {
            recvBuf[numBytes] = '\0';
            recvString += recvBuf;
            bytesRead += numBytes;
        }
    }
    cout << recvString << endl;
    close(sockFd);
}


int main(int argc, char *argv[]) {
    
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << "<grep options if any> [regex to grep for]" << endl;
        exit(EXIT_FAILURE);
    }

    string sendBuf;
    for (int i = 1; i < argc-1; i++) {
        sendBuf += argv[i];
        sendBuf += " ";
    }
    sendBuf += "'";
    sendBuf += argv[argc-1];
    sendBuf += "'";
   
    vector<thread> communicators;
    for (int i = 1; i <= 10 ; i++) {
        thread tmp{communicate, i, sendBuf};
        communicators.push_back(move(tmp));
    }

    for (auto &t : communicators)
        t.join();

    return 0;
}
