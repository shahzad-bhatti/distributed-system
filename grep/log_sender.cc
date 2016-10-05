#include <arpa/inet.h>
#include <cstring>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <netdb.h>
#include <string>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

constexpr int MAXDATASIZE = 5000;
constexpr uint16_t PORT = 5900;
constexpr int MYNAMELEN = 100;
constexpr int YES = 1;
constexpr int BACKLOG = 10;   // how many pending connections queue will hold


using namespace std;

int main(int argc, char *argv[]) {
    int sockFd, listenFd, numBytes;  // listen on Listenfd, new connection on sockFd
    struct sockaddr_in myAddr, theirAddr;
    socklen_t sockInSize = sizeof(theirAddr);
    
    string sendBuf;
    char recvBuf[MAXDATASIZE], str[INET_ADDRSTRLEN], myName[MYNAMELEN], machineNumber[3];

	if ((listenFd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        cerr << "socket: " << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }
    //set socket options to reuse same socket
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &YES, sizeof(int));

    memset(&myAddr, 0, sizeof(myAddr));
    myAddr.sin_family = AF_INET;
    myAddr.sin_addr.s_addr = INADDR_ANY;
    myAddr.sin_port = htons(PORT);
    if (bind(listenFd, (struct sockaddr *) &myAddr, sizeof(myAddr)) < 0) {
        cerr << "bind: " << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }
    
    if (listen(listenFd, BACKLOG) == -1) {
        cerr << "listen: " << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }

    if (gethostname(myName, MYNAMELEN) < 0) {
        cerr << "gethostname: " << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }
    cout << "My Name " << myName << endl; 
    memcpy(machineNumber, &myName[15], 2);
    machineNumber[2] = '\0';
    cout << "waiting for request..." << endl;
    
    while(1) {  // main accept() loop
        if ((sockFd = accept(listenFd, (struct sockaddr *)&theirAddr, &sockInSize)) < 0) {
            cerr << "accept: " << strerror(errno) << endl;
            exit(EXIT_FAILURE);
        }
        
        inet_ntop(AF_INET, &(theirAddr.sin_addr), str, INET_ADDRSTRLEN);
        cout << "got connection from " << str << endl;
        if ((numBytes = recv(sockFd, recvBuf, MAXDATASIZE - 1, 0)) < 0) {
            cerr << "recv: " << strerror(errno) << endl;
            exit(EXIT_FAILURE);
        }
        recvBuf[numBytes] = '\0';
        cout << "Received: " << recvBuf << endl;
        
        
        string grepString("grep ");
        grepString += recvBuf;
	    if (argc == 2) {
	        grepString += " ";
	        grepString += argv[1];
	    } else {
	        grepString += " machine.";
	        grepString += machineNumber;
	        grepString += ".log";
	    }

        FILE *popenFd;
        
        if(!(popenFd = popen(grepString.data(), "r"))) {
            cerr << "popen: " << strerror(errno) << endl;
            exit(EXIT_FAILURE);
        }
       // grepString += " | wc -l";
       // system(grepString.data());
        string sendString;
        size_t bytesRead = 0;
        char data[MAXDATASIZE+1];
        while((numBytes = read(fileno(popenFd), data, MAXDATASIZE)) > 0) {
            data[numBytes] = '\0';
            sendString += data;
            bytesRead += numBytes;
        }
        pclose(popenFd);
        if (send(sockFd, &bytesRead, sizeof(size_t), 0) == -1) {
            cerr << "send: " << strerror(errno) << endl;
            exit(EXIT_FAILURE);
        }
                
        if (send(sockFd,sendString.data() , sendString.size(), 0) == -1) {
            cerr << "send: " << strerror(errno) << endl;
            exit(EXIT_FAILURE);
        }
        sendBuf.clear();
    }
    close(listenFd);
    close(sockFd);
    return 0;
}
