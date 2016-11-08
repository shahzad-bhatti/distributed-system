/*
 * @file failure_detector.cc
 * @author Shahzad Bhatti
 * @date Sep 28, 2016
 *
 */
#include "failure_detector.h"
#include "../util/util.h"
#include "../sdfs/sdfs.h"

const string VMPREFIX = "fa16-cs425-g27-";

failureDetector::failureDetector(int number, string logFile, level sLevel, sdfs* fs)
: myNumber{number}, log{logFile} {

    log.setLevel(sLevel);
    myBirthTime = timeNow();
    log(INFO) << "My VM Number is " << myNumber;
    log(INFO) << "My ID " << myBirthTime;

    fill(ackRecvd.begin(), ackRecvd.end(), true);
    joinReply = false;
    log(INFO) << "Creating UDP socket.";
    createSocket();
    fillAddrs();
    list[myBirthTime] = myIP;
    fileSystem = fs;

    thread sendPingThread(&failureDetector::sendPING, this);
    sendPingThread.detach();  // let this run on its own

    thread recvMessagesThread(&failureDetector::recvMessages, this);
    recvMessagesThread.detach();  // let this run on its own

}

void failureDetector::recvMessages() {
    char recvBuf[MAXDATASIZE], sendBuf[MAXDATASIZE];
    int numBytes, size, dataLen, offset, senderNode;
    struct sockaddr_in theirAddr;
    socklen_t theirAddrLen = sizeof(theirAddr);

    while(1) {
        numBytes = recvfrom(sockFd, recvBuf, MAXDATASIZE, 0,(struct sockaddr *)&theirAddr, &theirAddrLen);
        recvBuf[numBytes] = '\0';
        senderNode = getNodeNumber(ntohl(theirAddr.sin_addr.s_addr));


        if (strncmp(recvBuf, "JOIN", 4) == 0) { // a new node sends JOIN message

            offset = 4;
            uint64_t theirBirthTime;
            memcpy(&theirBirthTime, recvBuf+offset, sizeof(theirBirthTime));
            theirBirthTime = ntohll(theirBirthTime);
            offset += sizeof(theirBirthTime);

            uint32_t theirIP;
            memcpy(&theirIP, recvBuf+offset, sizeof(theirIP));
            theirIP = ntohl(theirIP);
            offset += sizeof(theirIP);

            log(INFO) << "New node asking to join the system with ID " << theirBirthTime;
            list[theirBirthTime] = theirIP;  // update list
            log(INFO) << "Added " << theirBirthTime << " to the membership list";
            strcpy(sendBuf, "LIST");
            size = 4;

            dataLen = list.size() * (sizeof(myIP) + sizeof(myBirthTime));
            dataLen = htonl(dataLen);
            memcpy(sendBuf + size, &dataLen, sizeof(dataLen));
            size += sizeof(dataLen);

            fileSystem->newNode(senderNode);    // tell sdfs of new node

            uint64_t birthTime;
            uint32_t IP;
            int node;
            strcpy(recvBuf, "NEWN");
            // sending my list in response to join
            for (auto it=list.begin(); it!=list.end(); ++it) {

                if (it->first != myBirthTime && it->first != theirBirthTime) { // communicate new node to other nodes
                    node = getNodeNumber(it->second);
                    sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
                }
                birthTime = htonll(it->first);
                memcpy(sendBuf+size, &birthTime, sizeof(birthTime));
                size += sizeof(birthTime);

                IP = htonl(it->second);
                memcpy(sendBuf+size, &IP, sizeof(IP));
                size += sizeof(IP);
            }

            log(INFO) << "Sending my list in response to join request.";
            log(DEBUG) << "Bytes sending - " << size;
            sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&theirAddr, theirAddrLen);

        } else if(strncmp(recvBuf, "NEWN", 4) == 0) { // new node gossip about it

            offset = 4;
            uint64_t theirBirthTime;
            memcpy(&theirBirthTime, recvBuf+offset, sizeof(theirBirthTime));
            theirBirthTime = ntohll(theirBirthTime);
            offset += sizeof(theirBirthTime);

            uint32_t theirIP;
            memcpy(&theirIP, recvBuf+offset, sizeof(theirIP));
            theirIP = ntohl(theirIP);
            offset += sizeof(theirIP);

            auto it = list.find(theirBirthTime);
            if(it == list.end()) {  // if new then gossip about it
                log(INFO) << "New node with id " << theirBirthTime << " joined the system.";
                list[theirBirthTime] = theirIP;  // update list
                log(INFO) << "Added new node " << theirBirthTime << " to the list.";

                fileSystem->newNode(getNodeNumber(theirIP));    // tell sdfs of new node

                bool sentTo[NODES+1] = {};
                int node;
                auto newNode = getNodeNumber(theirIP);
                auto end = min(K, static_cast<int>(list.size() - 3));
                for (int i=0; i < end; ++i) {
                    do {
                        node = getRandomNode();
                    } while(sentTo[node] || node == newNode || node ==senderNode);
                    sentTo[node] = true;
                    sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
                }
            }
        } else if(strncmp(recvBuf, "LIST", 4) == 0) { // LIST in response to JOIN
            joinReply = true;
            log(INFO) << "Received a membership list in response to my join request.";
            dataLen = 0;
            offset = 4;
            memcpy(&dataLen, recvBuf+offset, sizeof(dataLen));
            dataLen = ntohl(dataLen);
            offset += sizeof(dataLen);

            log(DEBUG) << "Received LIST message has payload " << dataLen;

            if ( numBytes - dataLen != sizeof(int) + 4 ) {
                log(ERROR) << "Payload sent = " << dataLen << " payLoad received = " << numBytes - sizeof(dataLen) + 4;
                exit(4);
            }

            uint64_t birthTime;
            uint32_t IP;

            int members = dataLen/(sizeof(birthTime) + sizeof(IP));
            for (int i=0; i<members; ++i) {
                memcpy(&birthTime, recvBuf+offset, sizeof(birthTime));
                offset += sizeof(birthTime);

                memcpy(&IP, recvBuf+offset, sizeof(IP));
                offset += sizeof(IP);
                IP = ntohl(IP);
                list[ntohll(birthTime)] = IP;

                fileSystem->newNode(getNodeNumber(IP));    // tell sdfs of new node
            }
            log(INFO) << "Seccessfully joined the system.";

        } else if(strncmp(recvBuf, "LEAV", 4) == 0 || strncmp(recvBuf, "FAIL", 4) == 0 ) { // leave or fail message
            offset = 4;
            uint64_t birthTime;
            memcpy(&birthTime, recvBuf+offset, sizeof(birthTime));
            birthTime = ntohll(birthTime);

            auto it = list.find(birthTime);
            if (it != list.end()) {
                log(INFO) << birthTime << " has " << (strncmp(recvBuf, "LEAV", 4) == 0 ? "left the system." : "failed");

                updateSdfs(getNodeNumber(list[birthTime])); // tell sdfs of node failure

                list.erase(birthTime);
                log(INFO) << "Removed " << birthTime << " from my list";
                bool sentTo[NODES+1] = {};
                auto end = min(K, static_cast<int>(list.size() - 1));
                int node;
                for (int i=0; i < end; i++) {
                    do {
                        node = getRandomNode();
                    } while(sentTo[node]);
                    sentTo[node] = true;
                    log(INFO) << "Gossiping about the " << (strncmp(recvBuf, "LEAV", 4) == 0 ? "leaving of " : "failure of ") << birthTime;
                    sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
                }
            }
        } else if(strncmp(recvBuf, "PING", 4) == 0) {
            log(DEBUG2) << "Received PING message";
            strcpy(recvBuf, "ACKD");
            size = 4;
            copyMyID(recvBuf, size);
            sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&theirAddr, theirAddrLen);

        } else if(strncmp(recvBuf, "ACKD", 4) == 0) { // Direct ACK to PING

            offset = 4;
            uint64_t theirBirthTime;
            memcpy(&theirBirthTime, recvBuf+offset, sizeof(theirBirthTime));
            theirBirthTime = ntohll(theirBirthTime);
            offset += sizeof(theirBirthTime);

            uint32_t theirIP;
            memcpy(&theirIP, recvBuf+offset, sizeof(theirIP));
            theirIP = ntohl(theirIP);
            offset += sizeof(theirIP);

            int node = getNodeNumber(theirIP);
            ackRecvd[node] = true;

        } else if(strncmp(recvBuf, "PINR", 4) == 0) { // PING Request
            offset = 4;
            int target;
            memcpy(&target, recvBuf+size, sizeof(target));
            target = ntohl(target);
            strcpy(recvBuf, "PINI");
            sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&nodeAddrs[target], sizeof(nodeAddrs[target]));

        } else if(strncmp(recvBuf, "PINI", 4) == 0) { // PING indirect
            strcpy(recvBuf, "ACKI");
            sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&theirAddr, theirAddrLen);

        } else if(strncmp(recvBuf, "ACKI", 4) == 0) { // ACK indirect in response to indirect PING
            offset = 4;
            int requestor;
            offset += sizeof(requestor); // need to skip target
            memcpy(&requestor, recvBuf+size, sizeof(requestor));
            requestor = ntohl(requestor);

            strcpy(recvBuf, "ACKR");
            sendto(sockFd, recvBuf, numBytes, 0, (struct sockaddr*)&nodeAddrs[requestor], sizeof(nodeAddrs[requestor]));

        } else if(strncmp(recvBuf, "ACKR", 4) == 0) { // ACK in response to PING request
            offset = 4;
            int target;
            memcpy(&target, recvBuf+size, sizeof(target));
            target = ntohl(target);
            ackRecvd[target] = true;

        } else { // unrecongnized message
            log(ERROR) << "Unkown message: " << recvBuf;
            exit(2);
        }
    }
}

void failureDetector::updateFileSystem(int node) {
    fileSystem->nodeFailure(node); // tell sdfs of node failure
    
}

void failureDetector::updateSdfs(int node) {
    thread updateSdfsThread(&failureDetector::updateFileSystem, this, node);
    updateSdfsThread.detach();  // let this run on its own
}


void failureDetector::sendJOIN(int otherNode) {
    joinReply = false;
    char sendBuf[MAXDATASIZE];
    int size;

    strcpy(sendBuf, "JOIN");
    size = 4;
    copyMyID(sendBuf, size);

    struct timespec sleepFor;
    sleepFor.tv_sec = 0;
    sleepFor.tv_nsec = 500 * 1000 * 1000; // 0.5 sec
    log(INFO) << "Asking to join the system";
    while(1) {
        sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&nodeAddrs[otherNode], sizeof(nodeAddrs[otherNode]));
        log(DEBUG2) << "JOIN message sent";

        nanosleep(&sleepFor, 0);

        if (joinReply) {
            break;
        }
    }
}

void failureDetector::handleInput() {
    string input;
    while (1) {
        cin >> input;
        if (input.compare("join") == 0) {
            int otherNode;
            cin >> otherNode;
            sendJOIN(otherNode);

        } else if (input.compare("id") == 0) {
            cout << myBirthTime << endl;

        } else if (input.compare("list") == 0) {
            struct in_addr ipAddr;
            cout << "        ID             " << "IP\n";
            for (auto it=list.begin(); it!=list.end(); ++it) {
                ipAddr.s_addr = htonl(it->second);
                cout << it->first << "   " << inet_ntoa(ipAddr) << endl;
            }
        } else if (input.compare("leave") == 0) {
            leave();
            log (INFO) << "Leaving the system.";
            exit(1);

        } else {
            cout << "Wrong input: valid inputs are\n"
                 << "[list] to show current membership list\n"
                 << "[id] to show the id of this deamon\n"
                 << "[leave] to leave the system\n"
                 << "[join] <introducer vm's number> to join the system\n";
        }
    }
}

void failureDetector::createSocket() {
    //socket() and bind() our socket. We will do all sendto()ing and recvfrom()ing on this one.
    if((sockFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("socket");
        exit(7);
    }
    log(INFO) << "Socket created.";
    string myName = VMPREFIX;
    if (myNumber < 10) {
        myName += "0";
        myName += to_string(myNumber);
    } else {
        myName += to_string(myNumber);
    }
    myName += ".cs.illinois.edu";

    struct sockaddr_in bindAddr;
    memset(&bindAddr, 0, sizeof(bindAddr));
    bindAddr.sin_family = AF_INET;
    bindAddr.sin_port = htons(PORT);
    myIP = getIP(myName);

    struct in_addr tmp;
    tmp.s_addr = htonl(myIP);
    auto myIPStr = inet_ntoa(tmp);
    inet_pton(AF_INET, myIPStr, &bindAddr.sin_addr);

    // allow address reuse
    int YES = 1;
    setsockopt(sockFd, SOL_SOCKET, SO_REUSEADDR, &YES, sizeof(int));

    log(INFO) << "Binding socket.";
    if(bind(sockFd, (struct sockaddr*)&bindAddr, sizeof(struct sockaddr_in)) < 0) {
        perror("bind");
        close(sockFd);
        exit(6);
    }
    log(INFO) << "Socket binding done.";
    log(INFO) << "My IP address " << myIPStr << " ("<< myIP << ")";
}

void failureDetector::fillAddrs() {
    for(int i = 1; i <= NODES; i++) {
        string hostName = VMPREFIX;
        if (i < 10) {
            hostName += "0";
            hostName += to_string(i);
        } else {
            hostName += to_string(i);
        }
        hostName += ".cs.illinois.edu";
        memset(&nodeAddrs[i], 0, sizeof(nodeAddrs[i]));
        nodeAddrs[i].sin_family = AF_INET;
        nodeAddrs[i].sin_port = htons(PORT);
        IPAddrs[i] = getIP(hostName);

        struct in_addr tmp;
        tmp.s_addr = htonl(IPAddrs[i]);
        auto IP = inet_ntoa(tmp);
        log(DEBUG) << "IP address of " << i << " - " << IP << " ("<< IPAddrs[i] << ")";;
        inet_pton(AF_INET, IP, &nodeAddrs[i].sin_addr);
    }
}

void failureDetector::sendPING() {
    char sendBuf[MAXDATASIZE];
    struct timespec sleepFor;
    sleepFor.tv_sec = 0;
    int node, size = 4;
    uint64_t sentTime;
    strcpy(sendBuf, "PING");
    copyMyID(sendBuf, size);
    int intialSize = size;

    while(1) {
        if (list.size() > 1) {
            size = intialSize;
            node = getRandomNode();
            log(DEBUG2) << "sending PING to " << node;

            ackRecvd[node] = false;
            sentTime = timeNow();
            sentTime = htonll(sentTime);
            memcpy(&sentTime, sendBuf+size, sizeof(sentTime));
            size += sizeof(sentTime);

            sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
            sleepFor.tv_nsec = 500 * 1000 * 1000;
            nanosleep(&sleepFor, 0);
            if (!ackRecvd[node]) {
                log(INFO) << "Did not receive ACK from " << node;
                thread indirectThread(&failureDetector::sendIndirectPINGS, this, node);
                indirectThread.detach();  // let this run on its own
            }
        }
    }
}

void failureDetector::sendIndirectPINGS(int target) {
    struct timespec sleepFor;
    char sendBuf[MAXDATASIZE];
    int size = 4;
    int end = min(K, static_cast<int>(list.size()-2)); // we don't want to sent ping requests to self and target
    if (end < 1) {     // no other node to request pings, send another ping
        log(INFO) << "Sending a second ping to " << target;
        strcpy(sendBuf, "PING");
        copyMyID(sendBuf, size);
        uint64_t sentTime = timeNow();
        sentTime = htonll(sentTime);
        memcpy(&sentTime, sendBuf+size, sizeof(sentTime));
        size += sizeof(sentTime);

        sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&nodeAddrs[target], sizeof(nodeAddrs[target]));
        sleepFor.tv_sec = 0;
        sleepFor.tv_nsec = 500 * 1000 * 1000;
        nanosleep(&sleepFor, 0);
        if (!ackRecvd[target]) {
            log(INFO) << "Ping not received from " << target << " on second attempt";
            auto IP = IPAddrs[target];
            auto it =  list.begin();
            while (it != list.end() && it->second != IP) {
            ++it;
            }
            if (it != list.end()) {
                auto birthTime = it->first;
                log(INFO) << birthTime << " has failed.";

                updateSdfs(getNodeNumber(list[birthTime])); // tell sdfs of node failure

                list.erase(birthTime);
                log(INFO) << birthTime << " has been erased from the list.";
            }
        }
        return;
    }
    // if there are more than one other node, ask them to ping target
    strcpy(sendBuf, "PINR");

    int sentTarget = htonl(target);
    memcpy(sendBuf+size, &sentTarget, sizeof(sentTarget));
    size += sizeof(sentTarget);

    int sentMy = htonl(myNumber);
    memcpy(sendBuf+size, &sentMy, sizeof(sentMy));
    size += sizeof(sentMy);

    int node;
    for (int i=0; i<end; ++i) {
        do {
            node = getRandomNode();
        } while(node == target);
        sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
    }
    sleepFor.tv_sec = 0;
    sleepFor.tv_nsec = 500 * 1000 * 1000;
    nanosleep(&sleepFor, 0);
    if (!ackRecvd[target]) {
        auto IP = IPAddrs[target];
        auto it =  list.begin();
        while (it != list.end() && it->second != IP) {
            ++it;
        }
        if (it != list.end()) {
            auto birthTime = it->first;
            log(INFO) << birthTime << " has failed.";

            updateSdfs(getNodeNumber(list[birthTime])); // tell sdfs of node failure

            list.erase(birthTime);
            log(INFO) << birthTime << " has been erased from the list.";

            strcpy(sendBuf, "FAIL");
            size = 4;

            uint64_t sentBirthTime = htonll(birthTime);
            memcpy(sendBuf+size, &sentBirthTime, sizeof(sentBirthTime));
            size += sizeof(sentBirthTime);
            for (it = list.begin(); it != list.end(); ++it) {
                if (it->first != myBirthTime) {
                    node = getNodeNumber(it->second);
                    sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
                }
            }
        }
    }
}

void failureDetector::copyMyID(char* buf, int &size) {
    uint64_t copiedBirthTime = htonll(myBirthTime);
    memcpy(buf+size, &copiedBirthTime, sizeof(copiedBirthTime));
    size += sizeof(copiedBirthTime);

    uint32_t copiedIP = htonl(myIP);
    memcpy(buf+size, &copiedIP, sizeof(copiedIP));
    size += sizeof(copiedIP);
}

int failureDetector::getRandomNode() {
    int r;
    int node;
    auto it = list.begin();
    do {
        r = rand() % list.size(); // generate a random number between 0 and list.size()
        it = list.begin();
        advance(it, r);
        node = getNodeNumber(it->second);
    } while(node == myNumber);
    return node;
}

int failureDetector::getNodeNumber(uint32_t IP) {
    for(int i=1; i<=NODES; ++i) {
        if (IPAddrs[i]==IP)
            return i;
    }
    return 0;
}

uint32_t failureDetector::getIP(const string &hostname) {
    struct hostent *he;
    struct in_addr **addr_list;

    if ( !(he = gethostbyname( hostname.c_str() ) )) {
        // get the host info
        log(ERROR) << "Could not get IP from hostname";
        exit(8);
    }

    addr_list = (struct in_addr **) he->h_addr_list;
    for(int i = 0; addr_list[i] != nullptr; ++i) {
        // Return the first one;
        return ntohl(addr_list[i]->s_addr);
    }
    log(ERROR) << "Could not get IP from hostname";
    return 0;
}

void failureDetector::leave() {
    char sendBuf[MAXDATASIZE];
    strcpy(sendBuf, "LEAV");
    int node, size = 4;
    uint64_t sentBirthTime = htonll(myBirthTime);
    memcpy(sendBuf+size, &sentBirthTime, sizeof(sentBirthTime));
    size += sizeof(sentBirthTime);

    for (auto it=list.begin(); it !=list.end(); ++it) {
        if (it->first != myBirthTime) {
            node = getNodeNumber(it->second);
            sendto(sockFd, sendBuf, size, 0, (struct sockaddr*)&nodeAddrs[node], sizeof(nodeAddrs[node]));
        }
    }
}

uint64_t failureDetector::getBirthTime() {
    return  myBirthTime;
}

void failureDetector::printList() {
    struct in_addr ipAddr;
    cout << "        ID             " << "IP\n";
    for (auto it=list.begin(); it!=list.end(); ++it) {
        ipAddr.s_addr = htonl(it->second);
        cout << it->first << "   " << inet_ntoa(ipAddr) << endl;
    }
}
