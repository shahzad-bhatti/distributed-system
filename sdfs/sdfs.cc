/*
 * @file sdfs.cc
 * @date Oct 24, 2016
 *
 */
#include "sdfs.h"

#include <functional>
#include <iomanip>

const string VMPREFIX = "fa16-cs425-g27-";

sdfs::sdfs(int number, logger &logg)
: myNumber{number}, log(logg) {

    fd = new failureDetector(number, logg, this);
    createSocket();
    fill(ring.begin(), ring.end(), false);
    ring[number] = true;
    isAllFileNamesRecvd = false;
    isAllJuiceFilesRecvd = false;
    thread recvMessagesThread(&sdfs::recvMessages, this);
    recvMessagesThread.detach();  // let this run on its own
}

void sdfs::recvMessages() {

    char recvBuf[MAXDATASIZE];
    int numBytes, offset, senderNode;
    struct sockaddr_in theirAddr;
    socklen_t theirAddrLen = sizeof(theirAddr);

    listen(sockFd,10);

    // keep listening for incoming connections
    while(1) {
        int newConnFd = -1;
        newConnFd = accept(sockFd, (struct sockaddr *) &theirAddr, &theirAddrLen);

        if (newConnFd < 0) {
            perror("Cannot accept incoming connection");
        }

        numBytes = read(newConnFd, recvBuf, MAXDATASIZE - 1);
        recvBuf[numBytes] = '\0';
        senderNode = getNodeNumber(ntohl(theirAddr.sin_addr.s_addr));

        if (strncmp(recvBuf, "PUT", 3) == 0) { // put a file
            log(INFO) << "received a file to store";
            offset = 3;

            char label;
            memcpy(&label, recvBuf+offset, sizeof(label));
            offset += sizeof(label);

            auto fileName = recvFile(recvBuf, newConnFd, numBytes, offset);

            files.insert(pair<string,char>(fileName, label));

        } else if(strncmp(recvBuf, "GET", 3) == 0) { // Get a file
            log(INFO) << "received a request to send a file" << endl;
            offset = 3;

            char label;
            memcpy(&label, recvBuf+offset, sizeof(label));
            offset += sizeof(label);
            log(INFO) << "received a request to send a file with label " << label;

            int requestNode;
            memcpy(&requestNode, recvBuf+offset, sizeof(requestNode));
            offset += sizeof(requestNode);
            requestNode = ntohl(requestNode);

            int localNameSize;
            memcpy(&localNameSize, recvBuf+offset, sizeof(localNameSize));
            offset += sizeof(localNameSize);
            localNameSize = ntohl(localNameSize);

            int sdfsNameSize;
            memcpy(&sdfsNameSize, recvBuf+offset, sizeof(sdfsNameSize));
            offset += sizeof(sdfsNameSize);
            sdfsNameSize = ntohl(sdfsNameSize);

            char localName[localNameSize+1];
            memcpy(localName, recvBuf+offset, localNameSize);
            offset += localNameSize;
            localName[localNameSize] = '\0';

            char sdfsName[sdfsNameSize+1];
            memcpy(sdfsName, recvBuf+offset, sdfsNameSize);
            offset += sdfsNameSize;
            sdfsName[sdfsNameSize] = '\0';

            sendFile(requestNode, localName, sdfsName, label);

        } else if(strncmp(recvBuf, "DELT", 4) == 0) { // Delete the file
            log(INFO) << "received a request to delete a file";
            offset = 4;

            int fileNameSize;
            memcpy(&fileNameSize, recvBuf+offset, sizeof(fileNameSize));
            offset += sizeof(fileNameSize);
            fileNameSize = ntohl(fileNameSize);

            char fileName[fileNameSize+1];
            memcpy(fileName, recvBuf+offset, fileNameSize);
            offset += fileNameSize;
            fileName[fileNameSize] = '\0';

            log(INFO) << "received a request to delete " << fileName;
            removeFile(fileName);

        } else if(strncmp(recvBuf, "FILE", 4) == 0) { // FILE in response to GETT
            log(INFO) << "received the file in response to GET";
            offset = 4;
            recvFile(recvBuf, newConnFd, numBytes, offset);

        } else if(strncmp(recvBuf, "NFIL", 4) == 0) { // FILE does not exist in response to GETT
            offset = 4;

            int fileNameSize;
            memcpy(&fileNameSize, recvBuf+offset, sizeof(fileNameSize));
            offset += sizeof(fileNameSize);
            fileNameSize = ntohl(fileNameSize);

            char fileName[fileNameSize+1];
            memcpy(fileName, recvBuf+offset, fileNameSize);
            offset += fileNameSize;
            fileName[fileNameSize] = '\0';

        } else if(strncmp(recvBuf, "UPDA", 4) == 0) { // FILE does not exist in response to GETT
            offset = 4;

            char label = recvBuf[4];
            offset++;

            log(INFO) << "Update " << label << " message received";

            int fileCount;
            memcpy(&fileCount, recvBuf+offset, sizeof(fileCount));
            offset += sizeof(fileCount);
            fileCount = ntohl(fileCount);

            log(DEBUG) << "File count " << fileCount;

            int fileNameLen;
            for (int i=0; i < fileCount; i++) {
                memcpy(&fileNameLen, recvBuf+offset, sizeof(fileNameLen));
                offset += sizeof(fileNameLen);
                fileNameLen = ntohl(fileNameLen);

                string fileName(recvBuf+offset, fileNameLen);
                offset += fileNameLen;

                log(DEBUG) << "fileName " << fileName;

                auto it = files.find(fileName);
                if (it != files.end()) {
                    it->second = label;
                } else {
                    missingFiles.insert(pair<string, char>(fileName, label));
                    sendGetMessage(myNumber, senderNode, fileName, fileName, 'A');
                }
            }

        } else if(strncmp(recvBuf, "QURY", 4) == 0) { // check if this file exits
            offset = 4;
            log(INFO) << "received QURY message";

            int requestNode;
            memcpy(&requestNode, recvBuf+offset, sizeof(requestNode));
            offset += sizeof(requestNode);
            requestNode = ntohl(requestNode);

            log(DEBUG) << "request Node " << requestNode;

            int fileNameLen;
            memcpy(&fileNameLen, recvBuf+offset, sizeof(fileNameLen));
            offset += sizeof(fileNameLen);
            fileNameLen = ntohl(fileNameLen);

            string fileName(recvBuf+offset, fileNameLen);

            log(DEBUG) << "fileName " << fileName;

            auto it = files.find(fileName);
            if (it != files.end()) {
                sendExistMessage(requestNode, it->second);
                if (it->second != 'C') {
                    sendQueryMessage(requestNode, successorNode(myNumber), fileName);
                }
            }

        } else if(strncmp(recvBuf, "EXST", 4) == 0) { // reply for Query message if file exists
            offset = 4;

            char label = recvBuf[offset];

            struct in_addr tmp;
            tmp.s_addr = htonl(fd->IPAddrs[senderNode]);
            auto IP = inet_ntoa(tmp);
            cout << IP << "      " << label << endl;


        } else if(strncmp(recvBuf, "GEF", 3) == 0) { // request to get filenames given a prefix
            offset = 3;

            int sdfsPrefixLen;
            memcpy(&sdfsPrefixLen, recvBuf+offset, sizeof(sdfsPrefixLen));
            offset += sizeof(sdfsPrefixLen);
            sdfsPrefixLen = ntohl(sdfsPrefixLen);
            string dirPrefix(recvBuf+offset, sdfsPrefixLen);

            log() << "sdfs/ received GEF message from " << senderNode << " for " << dirPrefix;

            sendFileNames(dirPrefix, senderNode);

        } else if(strncmp(recvBuf, "FNAM", 4) == 0) { // response to GEF
            log() << "sdfs/ received FNAM message from " << senderNode;
            offset = 4;
            recvFileNames(recvBuf+4, senderNode);

        } else if(strncmp(recvBuf, "JSND", 4) == 0) { // request to send juice input files
            log() << "sdfs/ received a request to send juice input files";

            handleSendJuiceInputFiles(recvBuf+4);

        } else if(strncmp(recvBuf, "JFIL", 4) == 0) { // receive juice input file
            log(INFO) << "received a juice file from " << senderNode;
            offset = 4;
            recvJuiceFile(recvBuf, newConnFd, numBytes, offset);

        } else if(strncmp(recvBuf, "JSNT", 4) == 0) { // sent all juice input files
            log(INFO) << "received all juices file sent " << senderNode;
            handleAllJuiceFilesSent(senderNode);

        } else if(strncmp(recvBuf, "DELI", 4) == 0) { // delete intermediate files
            log(INFO) << "received delete intermediate files message from " << senderNode;
            handleDeleteIntermediateFiles(recvBuf+4);

        } else { // unrecongnized message
            log(ERROR) << "sdfs/ Unkown message: " << recvBuf;
            exit(2);
        }
        close(newConnFd);
    }
}

void sdfs::handleDeleteIntermediateFiles(char * buf) {
    int offset = 0;

    int prefixLen;
    memcpy(&prefixLen, buf+offset, sizeof(prefixLen));
    offset += sizeof(prefixLen);
    prefixLen = ntohl(prefixLen);
    
    string prefix(buf+offset, prefixLen);
    thread deleteIntermediateFilesThread(&sdfs::deleteIntermediateFiles, this, prefix);
    deleteIntermediateFilesThread.detach();  // let this run on its own

}

void sdfs::deleteIntermediateFiles(string prefix) {
    cout << "deleteing Intermediate Files\n";
    log() << "deleteing Intermediate Files\n";
    
    string sysCmd = "rm -f ";
    for (auto it=files.begin(); it!=files.end(); it++ ){
        if(isPrefix(prefix, it->first) ) {
            string cmd = sysCmd + it->first;
            system(cmd.c_str());
            files.erase(it);
        }
    }
    cout << "all intermediate files deleted\n";
    log() << "sdfs/ all intermediate files deleted";
}

void sdfs::handleAllJuiceFilesSent(int node) {
    juiceFilesNotifications.erase(node);

    if (juiceFilesNotifications.empty()) {
        lock_guard<mutex> lk(cvJuiceFilesMutex);
        isAllJuiceFilesRecvd = true;
        cout << "Notifying One for juice Files" << endl;
        log() << "sdfs/ Notify other thread that all juice Files have been received";
        cvJuiceFiles.notify_one();
    }
}

void sdfs::sendDeleteIntermediateFiles(string prefix) {
    char message[50];
    strcpy(message, "DELI");
    int offset = 4;

    int prefixLen = prefix.size();
    prefixLen = htonl(prefixLen);

    memcpy(message+offset, &prefixLen, sizeof(prefixLen));
    offset += sizeof(prefixLen);

    memcpy(message+offset, &prefix[0], prefix.size());
    offset += prefix.size();

    for (int node=1; node < NODES+1; node++) {
        if (ring[node]) {
            int connToServer;
            int status = connectToServer(node, &connToServer);
            if(status) {
                cout <<"deleteIntermediateFiles: Cannot connect to "<< node << endl;

            } else {
                write(connToServer, message, offset);
                close(connToServer);
            }
        }
    }
}


void sdfs::handleSendJuiceInputFiles(char* buf) {
    int offset = 0;

    int prefixLen;
    memcpy(&prefixLen, buf+offset, sizeof(prefixLen));
    prefixLen = ntohl(prefixLen);
    offset += sizeof(prefixLen);

    string prefix(buf+offset, prefixLen);
    offset += prefixLen;

    int countJuices;
    memcpy(&countJuices, buf+offset, sizeof(countJuices));
    countJuices = ntohl(countJuices);
    offset += sizeof(countJuices);

    int numJuices;
    memcpy(&numJuices, buf+offset, sizeof(numJuices));
    numJuices = ntohl(numJuices);
    offset += sizeof(numJuices);

    unordered_map<int, int> juiceIDs;
    for (int i = 0; i < numJuices; i++) {
        int juicerID;
        memcpy(&juicerID, buf+offset, sizeof(juicerID));
        juicerID = ntohl(juicerID);
        offset += sizeof(juicerID);

        int juicer;
        memcpy(&juicer, buf+offset, sizeof(juicer));
        juicer = ntohl(juicer);
        offset += sizeof(juicer);

        juiceIDs[juicerID] = juicer;
    }
    thread sendJuiceInputFilesThread(&sdfs::sendJuiceInputFiles, this, prefix, countJuices, juiceIDs);
    sendJuiceInputFilesThread.detach();  // let this run on its own
}

void sdfs::sendJuiceInputFiles(string prefix, int countJuices, unordered_map<int, int> juiceIDs) {
    int juicerID;
    log() << "sdfs/ sending Juice input files";
    cout << "sending Juice input files" << endl;

    for (auto it = files.begin(); it != files.end(); it++) {
        if(it->second == 'A' && isPrefix(prefix, it->first) ) {
            auto key = getKey(it->first);
            if (key.size() == 0) {
                continue;
            }
            juicerID =  hash<string>{}(key) % countJuices;
            auto it1 = juiceIDs.find(juicerID);

            if (it1 != juiceIDs.end()) {
                pushFileToNode(it1->second, it->first, prefix+"_"+key, "JFIL");
            }
        }
    }
    char message[10];
    strcpy(message, "JSNT");
    int offset = 4;
    for (auto it = juiceIDs.begin(); it != juiceIDs.end(); it++) {
        int connToServer;
        int status = connectToServer(it->second, &connToServer);
        if(status) {
            cout <<"sendJuiceInputFiles: Cannot connect to "<< it->second << endl;

        } else {
            write(connToServer, message, offset);
            close(connToServer);
        }
    }
    log() << "sdfs/ all juice input files sent";
    cout << "all juice input files sent" << endl;
}


void sdfs::recvJuiceFile(char * recvBuf, int connFd, int numBytes, int offset) {
    int fileNameSize;
    memcpy(&fileNameSize, recvBuf+offset, sizeof(fileNameSize));
    offset += sizeof(fileNameSize);
    fileNameSize = ntohl(fileNameSize);

    int length;
    memcpy(&length, recvBuf+offset, sizeof(length));
    offset += sizeof(length);
    length = ntohl(length);

    string fileName(recvBuf+offset, fileNameSize);
    offset += fileNameSize;

    if (fileName.size() == 0) {
        return;
    }

    ofstream wFile(fileName, ios::binary | ios::app);
    wFile.write(recvBuf + offset, numBytes - offset);

    length -= (numBytes - offset);
    while (length ) {
        numBytes = read(connFd, recvBuf, MAXDATASIZE - 1);
        recvBuf[numBytes] = '\0';
        wFile.write(recvBuf, numBytes);
        length -= numBytes;
    }
    juiceFiles.insert(fileName);
    wFile.close();
}


void sdfs::recvFileNames(char* recvBuf, int senderNode) {
    log() << "sdfs/ storing fileNames received from " << senderNode;
    int count;
    int offset = 0;
    memcpy(&count, recvBuf, sizeof(count));
    count = ntohl(count);
    offset += sizeof(count);

    int fileNameLen;
    for (int i=0; i < count; i++) {
        memcpy(&fileNameLen, recvBuf+offset, sizeof(fileNameLen));
        offset += sizeof(fileNameLen);
        fileNameLen = ntohl(fileNameLen);

        fileNames.insert(string(recvBuf+offset, fileNameLen));
        offset += fileNameLen;
    }

    log() << "sdfs/ " << count <<" fileName received from " << senderNode;

    cout << count <<" fileName received from " << senderNode << endl;
    fileNameRequestSent.erase(senderNode);

    if (fileNameRequestSent.empty()) {
        lock_guard<mutex> lk(cvMutex);
        isAllFileNamesRecvd = true;
        cout << "Notifying One" << endl;
        cv.notify_one();
    }
}


void sdfs::sendFileNames(string dirPrefix, int node) {
    char message[MAXDATASIZE];
    strcpy(message, "FNAM");
    int offset = 4;

    int count = 0;
    offset += sizeof(count);
    int fileNameLen;
    int sentFileNameLen;
    log() << "sdfs/ sending fileNames for " << dirPrefix;

    for (auto it=files.begin(); it!=files.end(); it++ ){
        if(it->second == 'A' && isPrefix(dirPrefix, it->first) ) {
            fileNameLen = it->first.size();
            sentFileNameLen = htonl(fileNameLen);
            memcpy(message+offset, &sentFileNameLen, sizeof(sentFileNameLen));
            offset += sizeof(sentFileNameLen);

            memcpy(message+offset, &it->first[0], fileNameLen);
            offset += fileNameLen;
            count++;
        }
    }
    log(DEBUG) << "sdfs/ total fileNames sending " << count;
    count = htonl(count);
    memcpy(message+4, &count, sizeof(count));

    int connToServer;
    int status = connectToServer(node, &connToServer);
    if(status) {
        cout <<"sendExistMessage: Cannot connect to "<< node << endl;

    } else {
        write(connToServer, message, offset);
        close(connToServer);
    }
    log() << "fileNames sent for " << dirPrefix;
}


void sdfs::getFileNames(string dirPrefix) {
    log() << "sdfs/ sending Get file names message for dir " << dirPrefix;
    sendGetFileNamesMessage(dirPrefix);

    unique_lock<mutex> lk(cvMutex);
    log() << "sdfs/ waiting for fileNames for " << dirPrefix;
    cout << "Waiting... \n";
    cv.wait(lk, [this]{return isAllFileNamesRecvd;});
}


void sdfs::sendGetFileNamesMessage(string dirPrefix) {
    isAllFileNamesRecvd = false;
    int sdfsPrefixLen = dirPrefix.size();
    int sentSdfsPrefixLen = htonl(sdfsPrefixLen);

    char message[MAXDATASIZE];
    strcpy(message, "GEF");
    int offset = 3;

    memcpy(message+offset, &sentSdfsPrefixLen, sizeof(sentSdfsPrefixLen));
    offset += sizeof(sentSdfsPrefixLen);

    memcpy(message+offset, &dirPrefix[0], sdfsPrefixLen);
    offset += sdfsPrefixLen;

    log(DEBUG) << "GEF" << " message Length " << offset;

    for (size_t hostNode = 1; hostNode < ring.size(); hostNode++) {
        if (ring[hostNode]) {
            fileNameRequestSent.insert(hostNode);
            int connToServer;
            int status = connectToServer(hostNode, &connToServer);
            if(status) {
                cout <<"sendGetMessage: Cannot connect to "<< hostNode << endl;
            } else {
                log(DEBUG) << "sdfs/ sending Get file Names message to " << hostNode;
                write(connToServer, message, offset);
                close(connToServer);
            }
        }
    }
}


void sdfs::fetchJuiceInputFiles() {
    unique_lock<mutex> lk(cvJuiceFilesMutex);
    log() << "sdfs/ waiting for juice files";
    cout << "Waiting for juice input files... \n";
    cvJuiceFiles.wait(lk, [this]{return isAllJuiceFilesRecvd;});
    log() << "sdfs/ all juice files have been received.";
    isAllJuiceFilesRecvd = false;
}


void sdfs::sendJuiceFilesToJuicers(string prefix, int countJuices, unordered_map<int, int> juiceIDs) {
    char message[MAXDATASIZE];
    strcpy(message, "JSND");
    int offset = 4;

    int prefixLen = prefix.size();
    prefixLen = htonl(prefixLen);

    memcpy(message+offset, &prefixLen, sizeof(prefixLen));
    offset += sizeof(prefixLen);

    memcpy(message+offset, &prefix[0], prefix.size());
    offset += prefix.size();

    countJuices = htonl(countJuices);
    memcpy(message+offset, &countJuices, sizeof(countJuices));
    offset += sizeof(countJuices);

    int numJuices = juiceIDs.size();
    numJuices = htonl(numJuices);

    memcpy(message+offset, &numJuices, sizeof(numJuices));
    offset += sizeof(numJuices);

    for (auto it = juiceIDs.begin(); it != juiceIDs.end(); it++) {
        int juicerID = htonl(it->first);
        memcpy(message+offset, &juicerID, sizeof(juicerID));
        offset += sizeof(juicerID);

        int juicer = htonl(it->second);
        memcpy(message+offset, &juicer, sizeof(juicer));
        offset += sizeof(juicer);
    }

    for (int node = 1; node < NODES+1; node++) {
        if(ring[node]) {
            int connToServer;
            int status = connectToServer(node, &connToServer);
            if(status) {
                cout <<"sendJuiceIputMessages: Cannot connect to "<< node << endl;
            } else {
                write(connToServer, message, offset);
                close(connToServer);
            }
        }
    }
    log() << "sdfs/ Juice input request messages sent";
    cout << "Juice input request messages sent" << endl;
}


void sdfs::recvMapleFiles() {
    isAllMapleFilesRecvd = false;
    {
        lock_guard<mutex> lk(mapleFilesMutex);
            for (auto it = mapleFiles.begin(); it !=mapleFiles.end(); it++) {
            auto hostNode = location(*it);
            if (hostNode == myNumber) {
                recvdMapleFiles.insert(*it);
                mapleFiles.erase(it);
            }
        }
    }
    unique_lock<mutex> lk(cvMapleFilesMutex);
    log() << "sdfs/ waiting for mapleFiles";
    cout << "Waiting for MapleFiles... \n";
    cvMapleFiles.wait(lk, [this]{return isAllMapleFilesRecvd;});
    log() << "sdfs/ all maple Files have been received.";
}


string sdfs::recvFile(char * recvBuf, int connFd, int numBytes, int offset) {
    int fileNameSize;
    memcpy(&fileNameSize, recvBuf+offset, sizeof(fileNameSize));
    offset += sizeof(fileNameSize);
    fileNameSize = ntohl(fileNameSize);

    log(DEBUG) << "Receiving file";
    log(DEBUG) << "fileNameSize " << fileNameSize;

    int length;
    memcpy(&length, recvBuf+offset, sizeof(length));
    offset += sizeof(length);
    length = ntohl(length);

    log(DEBUG) << "length of the file " << length;

    string fileName(recvBuf+offset, fileNameSize);
    offset += fileNameSize;

    log(DEBUG) << "fileName " << fileName;

    ofstream wFile(fileName, ios::binary | ios::trunc);
    wFile.write(recvBuf + offset, numBytes - offset);

    length -= (numBytes - offset);

    while (length ) {
        numBytes = read(connFd, recvBuf, MAXDATASIZE - 1);
        recvBuf[numBytes] = '\0';
        wFile.write(recvBuf, numBytes);
        length -= numBytes;
    }

    // if this File is one of the missing files.
    auto it = missingFiles.find(fileName);
    if (it != missingFiles.end()) {
        files.insert(pair<string, char>(it->first, it->second));
        missingFiles.erase(fileName);
    }

    wFile.close();
    log(INFO) << "stored file on local disk";

    lock_guard<mutex> lk(mapleFilesMutex);
    if (!mapleFiles.empty()) {
        auto it = mapleFiles.find(fileName);
        if (it != mapleFiles.end()) {
            recvdMapleFiles.insert(fileName);
            mapleFiles.erase(it);
        }
        if (mapleFiles.empty()) {
            lock_guard<mutex> lk(cvMapleFilesMutex);
            isAllMapleFilesRecvd = true;
            cout << "Notifying One for Maple Files" << endl;
            log() << "sdfs/ Notify other thread that all mapleFiles have been received";
            cvMapleFiles.notify_one();

        }
    }
    return fileName;
}


void sdfs::sendExistMessage(int node, char label) {
    int connToServer;
    int status = connectToServer(node, &connToServer);
    if(status) {
        cout <<"sendExistMessage: Cannot connect to "<< node << endl;
    } else {
        char message[MAXDATASIZE];
        strcpy(message, "EXST");
        int offset = 4;

        message[offset] = label;
        offset++;

        write(connToServer, message, offset);
        close(connToServer);
    }
}


void sdfs::sendFileToSuccessor(string fileName, char label) {
    auto node = successorNode(myNumber);
    string code("PUT");
    code += label;
    pushFileToNode(node, fileName, fileName, code);
}


void sdfs::sendFile(int requestNode, string localName, string sdfsName, char label) {
    auto it = files.find(localName);
    if (it != files.end()) {
        pushFileToNode(requestNode, localName, sdfsName, "FILE");

    } else if (label != 'C') {
        sendGetMessage(requestNode, successorNode(myNumber), sdfsName, localName, label+1);

    } else {
        int connToServer;
        int status = connectToServer(requestNode, &connToServer);
        if(status) {
            cout <<"sendFile: Cannot connect to "<< requestNode << endl;

        } else {
            int localNameLen = localName.size();
            int sentLocalNameLen = htonl(localNameLen);

            char message[MAXDATASIZE];
            strcpy(message, "NFIL");
            int offset = 4;

            memcpy(message+offset, &sentLocalNameLen, sizeof(sentLocalNameLen));
            offset += sizeof(sentLocalNameLen);

            memcpy(message+offset, &localName[0], localNameLen);
            offset += localNameLen;

            write(connToServer, message, offset);
            close(connToServer);
        }
    }
}


void sdfs::fetchFile(string sdfsName, string localName) {
    auto hostNode = location(sdfsName);

    log(INFO) << "fetching file " << sdfsName <<  ", hosting node " << hostNode;
    if (hostNode == myNumber) {
        if (sdfsName.compare(localName) != 0) {
            ifstream  src(sdfsName, ios::binary);
            ofstream  dst(localName, ios::binary);
            dst << src.rdbuf();
            src.close();
            dst.close();
        }
        return;
    }
    sendGetMessage(myNumber, hostNode, sdfsName, localName, 'A');
}


void sdfs::sendGetMessage(int requestNode, int hostNode, string sdfsName, string localName, char label) {
    int connToServer;
    int status = connectToServer(hostNode, &connToServer);
    if(status) {
        cout <<"sendGetMessage: Cannot connect to "<< hostNode << endl;

    } else {
        int sdfsNameLen = sdfsName.size();
        int localNameLen = localName.size();
        int sentSdfsNameLen = htonl(sdfsNameLen);
        int sentLocalNameLen = htonl(localNameLen);

        char message[MAXDATASIZE];
        strcpy(message, "GET");
        int offset = 3;

        message[offset] = label;
        offset++;

        requestNode = htonl(requestNode);
        memcpy(message+offset, &requestNode, sizeof(requestNode));
        offset += sizeof(requestNode);

        memcpy(message+offset, &sentSdfsNameLen, sizeof(sentSdfsNameLen));
        offset += sizeof(sentSdfsNameLen);

        memcpy(message+offset, &sentLocalNameLen, sizeof(sentLocalNameLen));
        offset += sizeof(sentLocalNameLen);

        memcpy(message+offset, &sdfsName[0], sdfsNameLen);
        offset += sdfsNameLen;

        memcpy(message+offset, &localName[0], localNameLen);
        offset += localNameLen;

        log(DEBUG) << "GET" << label << " message Length " << offset;

        write(connToServer, message, offset);
        close(connToServer);
    }
}


bool sdfs::storeFile(string localName, string sdfsName) {
    auto node = location(sdfsName);
    vector<int> nodes;
    vector<string> messageTypes; 
    if (node == myNumber) {
        if (localName.compare(sdfsName) != 0) {
            ifstream  src(localName, ios::binary);
            ofstream  dst(sdfsName, ios::binary);
            dst << src.rdbuf();
            src.close();
            dst.close();
        }
        files.insert(pair<string, char>(sdfsName, 'A'));
        
        node = successorNode(node);     
        nodes.push_back(node);          //B
        node = successorNode(node);
        nodes.push_back(node);          //C
        
        messageTypes.push_back("PUTB");
        messageTypes.push_back("PUTC");
        return pushFileToNodes(nodes, localName, sdfsName, messageTypes);
    }
    if (successorNode(node) == myNumber) {
        if (localName.compare(sdfsName) != 0) {
            ifstream  src(localName, ios::binary);
            ofstream  dst(sdfsName, ios::binary);
            dst << src.rdbuf();
            src.close();
            dst.close();
        }
        files.insert(pair<string, char>(sdfsName, 'B'));
        
        nodes.push_back(node);                              //A
        messageTypes.push_back("PUTA");

        nodes.push_back(successorNode(myNumber));          //C
        messageTypes.push_back("PUTC");

        return pushFileToNodes(nodes, localName, sdfsName, messageTypes);
    }

    if (successorNode(successorNode(node)) == myNumber) {
        if (localName.compare(sdfsName) != 0) {
            ifstream  src(localName, ios::binary);
            ofstream  dst(sdfsName, ios::binary);
            dst << src.rdbuf();
            src.close();
            dst.close();
        }
        files.insert(pair<string, char>(sdfsName, 'C'));
        
        nodes.push_back(node);                          //A
        messageTypes.push_back("PUTA");

        nodes.push_back(successorNode(node));          //B
        messageTypes.push_back("PUTB");

        return pushFileToNodes(nodes, localName, sdfsName, messageTypes);
        
    }

    nodes.push_back(node);          //A
    node = successorNode(node);
    nodes.push_back(node);          //B
    node = successorNode(node);
    nodes.push_back(node);          //C
    
    messageTypes.push_back("PUTA");
    messageTypes.push_back("PUTB");
    messageTypes.push_back("PUTC");
    return pushFileToNodes(nodes, localName, sdfsName, messageTypes);
 
}


void sdfs::removeFile(string fileName) {
    auto it = files.find(fileName);
    if (it != files.end()) {
        auto label = it->second;
        files.erase(it);

        string cmd = "rm -f " + fileName;
        system(cmd.c_str());

        if (label != 'C') {
            sendDeleteMessage(successorNode(myNumber), fileName);
        }
    }
}


void sdfs::deleteFile(string fileName) {
    auto node = location(fileName);

    if (node == myNumber) {
        removeFile(fileName);
        return;
    }
    sendDeleteMessage(node, fileName);
}


void sdfs::sendDeleteMessage(int node, string fileName) {
    int connToServer;
    int status = connectToServer(node, &connToServer);
    if(status) {
        cout <<"sendDeleteMessage: Cannot connect to "<< node << endl;

    } else {
        int fileNameLen = sizeof(fileName);
        int sentFileNameLen = htonl(fileNameLen);

        char message[MAXDATASIZE];
        strcpy (message, "DELT");
        int offset = 4;
        memcpy (message+offset, &sentFileNameLen, sizeof(sentFileNameLen));
        offset += sizeof(sentFileNameLen);
        memcpy (message+offset, &fileName[0], fileNameLen);
        offset += fileNameLen;
        write(connToServer, message, offset);
        close(connToServer);
    }
}


void sdfs::newNode(int node) {
    ring[node] = true;
}


void sdfs::updateFileIds() {
    for (auto it = files.begin(); it != files.end(); ++it) {
        auto filename = it->first;
        auto node = location(filename);

        if (myNumber == node) {
            files[filename] = 'A';
        }
    }
}


int sdfs::createUpdaMsg(char* msg, vector<string>& filenames, char fileType) {

    string msgType = "UPDA";
    msgType += fileType;

    memcpy(msg, &msgType[0], msgType.size());
    int bufferlen = msgType.size();

    int fileCt = htonl(filenames.size());
    memcpy(msg+bufferlen, &fileCt, sizeof(fileCt));
    bufferlen += sizeof(fileCt);

    for (string filename : filenames) {
        //filename size
        int filenameSize = htonl(filename.size());
        memcpy(msg+bufferlen, &filenameSize, sizeof(filenameSize));
        bufferlen += sizeof(filenameSize);

        //filename
        memcpy(msg+bufferlen, &filename[0], filename.size());
        bufferlen += filename.size();
    }
    log(DEBUG) << "update buffer size " << bufferlen;
    return bufferlen;
}


void sdfs::requestUpdateMasteringFiles() {
    vector<string> masteringFiles;
    for (auto it = files.begin(); it != files.end(); ++it) {
        if (it->second == 'A') {
            masteringFiles.push_back(it->first);
        }
    }

    if (masteringFiles.empty()) {
        return;
    }

    // send request to potential b/c
    // msg: UPDA, fileCT, sizeof(filename1), filename1 ...
    int targetNode = myNumber;

    for (char fileType = 'B'; fileType<='C'; ++fileType) {
        char msg[MAXDATASIZE2];
        auto offset = createUpdaMsg(msg, masteringFiles, fileType);

        //send to appropriate node
        targetNode = successorNode(targetNode);
        if (targetNode == myNumber) {
            return;
        }
        int connToServer;
        int status = connectToServer(targetNode, &connToServer);
        if(status) {
            cout <<"requestUpdateMasteringFiles: Cannot connect to "<< targetNode << endl;

        } else {
            write(connToServer, msg, offset);
            close(connToServer);
        }
    }
}


void sdfs::updateFileDistribution() {
    lock_guard<mutex> lck (updateFileDistMutex);
    updateFileIds();
    requestUpdateMasteringFiles();
}


void sdfs::nodeFailure(int node) {
    if (!ring[node]) {
        return;
    }

    if ( node == predecessorNode(myNumber) ||
         node == successorNode(myNumber) ||
         node == successorNode( successorNode(myNumber) ) ) {

        ring[node] = false;
        log(INFO) << "node " << node << " fails, updated file ids." << endl;
        updateFileDistribution();
    }
    ring[node] = false;
}


void sdfs::showStore() {
    cout<<"=> showStore: \n";
    const char separator    = ' ';
    const int nameWidth     = 20;
    const int charWidth      = 1;
    for (auto it=files.begin(); it!=files.end(); ++it) {
        cout << left << setw(nameWidth) << setfill(separator) << it->first;
        cout << left << setw(charWidth) << setfill(separator) << it->second << endl;
    }
}


void sdfs::showFileLocations(string fileName){
    auto node = location(fileName);
    if (node == myNumber) {
        auto it = files.find(fileName);
        if (it != files.end()) {
            struct in_addr tmp;
            tmp.s_addr = htonl(fd->IPAddrs[node]);
            auto IP = inet_ntoa(tmp);
            cout << IP << "      " << it->second << endl;
            if (it->second == 'C') {
                return;
            }
            sendQueryMessage(myNumber, successorNode(myNumber), fileName);
        }
        return;
    }
    sendQueryMessage(myNumber, node, fileName);
}


void sdfs::sendQueryMessage(int requestNode, int hostNode, string fileName) {
    int connToServer;
    int status = connectToServer(hostNode, &connToServer);
    if(status) {
        cout << "sendQueryMessage: Cannot connect to "<< hostNode << endl;

    } else {
        char message[MAXDATASIZE];
        strcpy(message, "QURY");
        int offset = 4;

        requestNode = htonl(requestNode);
        memcpy(message+offset, &requestNode, sizeof(requestNode));
        offset += sizeof(requestNode);

        int fileNameLen = htonl(fileName.size());
        memcpy(message+offset, &fileNameLen, sizeof(fileNameLen));
        offset += sizeof(fileNameLen);

        memcpy(message+offset, &fileName[0], fileName.size());
        offset += fileName.size();
        log(INFO) << "sending Query message " << offset << endl;
        write(connToServer, message, offset);
        close(connToServer);
    }
}


void sdfs::handleInput() {
    // handleInput for both failure_detector and sdfs
    string input;
    while (1) {
        cin >> input;
        if (input.compare("join") == 0) {
            int otherNode;
            cin >> otherNode;
            fd->sendJOIN(otherNode);

        } else if (input.compare("id") == 0) {
            cout << fd->getBirthTime() << endl;

        } else if (input.compare("list") == 0) {
            fd->printList();

        } else if (input.compare("leave") == 0) {
            fd->leave();
            log (INFO) << "Leaving the system.";
            exit(1);

        } else if (input.compare("put") == 0) {
            string localFileName, sdfsFileName;
            cin >> localFileName >> sdfsFileName;
            storeFile(localFileName, sdfsFileName);

        } else if (input.compare("get") == 0) {
            string localFileName, sdfsFileName;
            cin >> sdfsFileName >> localFileName;
            fetchFile(sdfsFileName, localFileName);

        } else if (input.compare("delete") == 0) {
            string fileName;
            cin >> fileName;
            deleteFile(fileName);

        } else if (input.compare("store") == 0) {
            showStore();

        } else if (input.compare("ls") == 0) {
            string fileName;
            cin >> fileName;

            showFileLocations(fileName);

        } else if (input.compare("ring") == 0) {
            printRing();

        } else if (input.compare("next") == 0) {
            cout << successorNode(myNumber) << endl;

        } else {
            cout << "Wrong input: valid inputs are\n"
                 << "[list] to show current membership list\n"
                 << "[id] to show the id of this deamon\n"
                 << "[leave] to leave the system\n"
                 << "[join] <introducer vm's number> to join the system\n"
                 << "[put] <localFileName> <remoteFile> to add put file to sdfs\n"
                 << "[delete] <remoteFile> to delete file to sdfs\n"
                 << "[store] to show all files at this location\n"
                 << "[ls] <remoteFile> to show file replica locations\n";
        }
    }
}


void sdfs::createSocket() {

    //socket() and bind() our socket. We will do all recv()ing on this socket.

    struct sockaddr_in serv_addr;

    sockFd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockFd < 0) {
        perror("Cannot open socket");
    }
    // allow address reuse
    int YES = 1;
    setsockopt(sockFd, SOL_SOCKET, SO_REUSEADDR, &YES, sizeof(int));

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT2);

    log(INFO) << "Binding socket.";
    if (bind(sockFd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        perror("Cannot bind");
        close(sockFd);
    }
    log(INFO) << "Socket binding done.";
}


int sdfs::getNodeNumber(uint32_t IP) {
    for(int i = 1; i <= NODES; ++i) {
        if (fd->IPAddrs[i]==IP)
            return i;
    }
    return 0;
}


int sdfs::connectToServer(int targetNode, int *connectionFd) {

    struct in_addr tmp;
    tmp.s_addr = htonl(fd->IPAddrs[targetNode]);
    auto IP = inet_ntoa(tmp);

    //create client skt
    *connectionFd = socket(AF_INET, SOCK_STREAM, 0);

    // allow address reuse
    int YES = 1;
    setsockopt(sockFd, SOL_SOCKET, SO_REUSEADDR, &YES, sizeof(int));

    // open socket
    if(*connectionFd < 0) {
        cout << "Cannot open socket" << endl;
        exit(1);
    }

    //Variables Declaration
    struct addrinfo hints, * res;
    int status;

    //clear hints
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    auto portStr = to_string(PORT2);
    status = getaddrinfo(IP, portStr.c_str(), &hints, &res);
    if (status != 0) {
        fprintf(stderr, "Error getaddrinfo\n");
        exit(1);
    }

    status = connect(*connectionFd, res->ai_addr, res->ai_addrlen);
    if (status < 0) {
        cout << "Cannot connect to: " << targetNode << endl;
        cout.flush();
        return -1;
    }
    return 0;
}


bool sdfs::pushFileToNode(int targetNode, string localFile, string remoteFile, string code) {
    int connectionToServer;

    std::ifstream file(localFile, ios::binary);

    if (!file.good()) {
        cout << "pushFileToNode: Could not open file: " << localFile << endl;
        return false;
    }

    log(INFO) << "pushFileToNode: Connecting to "<< targetNode << "..." << endl;

    int ret = connectToServer(targetNode, &connectionToServer);

    if(ret!=0) {
        cout <<"ERROR pushFileToNode: Cannot connect to " << targetNode << endl;
        file.close();
        return false;

    } else {

        // Read the file, store in buffer
        file.seekg (0, file.end);
        int length = file.tellg();
        file.seekg (0, file.beg);
      //  char * buffer = new char [length];
      //  file.read (buffer,length);

        int sentLength = htonl(length);
        int fileNameSize = remoteFile.size();
        int sentFileNameSize = htonl(fileNameSize);

        // header: PUTA, sizeof(filename), sizeof(content), filename
        char header[MAXDATASIZE];
        int offset = 4;
        memcpy(header, &code[0], 4);

        memcpy(header+offset, &sentFileNameSize, sizeof(sentFileNameSize));
        offset += sizeof(sentFileNameSize);

        memcpy(header+offset, &sentLength, sizeof(sentLength));
        offset += sizeof(sentLength);

        memcpy(header+offset, &remoteFile[0], fileNameSize);
        offset += fileNameSize;

        header[offset] = '\0';

        char* message = new char[offset + length];

        memcpy(message, header, offset);

        file.read(&message[offset], length);

        write(connectionToServer, message, offset + length );
        close(connectionToServer);
        file.close();
        delete[] message;
        return true;
    }
}


bool sdfs::pushFileToNodes(vector<int> nodes, string localFile, string remoteFile, vector<string> codes) {
    std::ifstream file(localFile, ios::binary);

    if (!file.good()) {
        cout << "pushFileToNodes: Could not open file: " << localFile << endl;
        return false;
    }
         
    // Read the file, store in buffer
    file.seekg (0, file.end);
    int length = file.tellg();
    file.seekg (0, file.beg);

    int sentLength = htonl(length);
    int fileNameSize = remoteFile.size();
    int sentFileNameSize = htonl(fileNameSize);

    // header: PUTA, sizeof(filename), sizeof(content), filename
    char header[MAXDATASIZE];
    int offset = 4;
    string code = codes[0];
    strcpy(header, code.data());

    memcpy(header+offset, &sentFileNameSize, sizeof(sentFileNameSize));
    offset += sizeof(sentFileNameSize);

    memcpy(header+offset, &sentLength, sizeof(sentLength));
    offset += sizeof(sentLength);

    memcpy(header+offset, &remoteFile[0], fileNameSize);
    offset += fileNameSize;

    header[offset] = '\0';

    char* message = new char[offset + length];

    memcpy(message, header, offset);

    file.read(&message[offset], length);

    for (size_t i=0; i < nodes.size(); i++) {
        code = codes[i];
        strcpy(message, code.data());
        int connectionToServer;
        int ret = connectToServer(nodes[i], &connectionToServer);
        if(ret!=0) {
            cout <<"ERROR pushFileToNodes: Cannot connect to " << nodes[i] << endl;
            file.close();
            return false;

        } else {

            int ret = write(connectionToServer, message, offset + length);
            if (ret !=0) {
                close(connectionToServer);
                file.close();
                delete[] message;
                return false;
            }
            close(connectionToServer);
        }
    
    }           
    file.close();
    delete[] message;
    return true;
}


void sdfs::printRing() {
    for (int i=1; i <= NODES; i++) {
        if (ring[i]) {
            cout << i << endl;
        }
    }
}


int sdfs::location(const string &filename) {
    int node =  hash<string>{}(filename) % 10;
    if (node == 0) {
        node = 10;
    }
    if (!ring[node]) {
        node = successorNode(node);
    }
    return node;
}


int sdfs::successorNode(int node) {
    int pos = 0;
    for (int i = node+1; ; ++i) {
        pos = i % 11;
        if (ring[pos]) {
            return pos;
        }
    }
}


int sdfs::predecessorNode(int node) {
    int pos = node -1;
    while(1) {
        if (pos == 0) {
            pos = 10;
        }
        if (ring[pos]) {
            return pos;
        }
        --pos;
    }
}

