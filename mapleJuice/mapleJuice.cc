
/*
 * @file MapleJuice.cc
 * @date Nov 29, 2016
 *
 */

#include "mapleJuice.h"
#include "../sdfs/sdfs.h"
#include "../util/util.h"

#include <functional>
#include <iomanip>
#include <cctype>
#include <chrono>

const string VMPREFIX = "fa16-cs425-g27-";


mapleJuice::mapleJuice(int number, logger & logg)
: fs{number, logg}, log(logg) {

    createSocket();
    isMaster = false;
}


void mapleJuice::recvMessages() {
    char recvBuf[MAXDATASIZE];
    int numBytes, offset, senderNode;
    struct sockaddr_in theirAddr;
    socklen_t theirAddrLen = sizeof(theirAddr);

    listen(sockFd, 10);

    // keep listening for incoming connections
    while(1) {
        int newConnFd = -1;
        newConnFd = accept(sockFd, (struct sockaddr *) &theirAddr, &theirAddrLen);

        if (newConnFd < 0) {
            perror("Cannot accept incoming connection");
        }

        numBytes = read(newConnFd, recvBuf, MAXDATASIZE - 1);
        recvBuf[numBytes] = '\0';
        senderNode = fs.getNodeNumber(ntohl(theirAddr.sin_addr.s_addr));

        if (strncmp(recvBuf, "MAPL", 4) == 0) { // Maple jobs
            log(INFO) << "mapleJuice/ received a maple job";

            handleMapleJob(recvBuf+4, senderNode);

        } else if (strncmp(recvBuf, "MAPD", 4) == 0) { // maple job is done by a worker
            log(INFO) << "mapleJuice/ node " << senderNode << " has finished maple job";
            handleMapleJobDone(senderNode);

        } else if(strncmp(recvBuf, "JUIC", 4) == 0) { // Juice jobs{
            log(INFO) << "mapleJuice/ received a juice job";
            handleJuiceJob(recvBuf+4, senderNode);

        } else if (strncmp(recvBuf, "JUID", 4) == 0)  { //Some Juice job is done by one juicer
            log(INFO) << "mapleJuice/ node " << senderNode << " has finished juice job";
            handleJuiceJobDone(senderNode);

        } else if (strncmp(recvBuf, "FAMJ", 4) == 0)  {
            int offset = 4;
            int failNode;
            memcpy(&failNode, recvBuf+offset, sizeof(failNode));
            failNode = ntohl(failNode);

            cout<< "node " << failNode << " failed" << endl;
            if (isMaster) {
                handleFailure(failNode);
            }
            
        } else {
        // unrecongnized message
            log(ERROR) << "Unkown message: " << recvBuf;
            exit(2);
        }
        close(newConnFd);

    }
}


void mapleJuice::handleFailure(int failNode) {
    cout << "MJ master handing failure " << failNode << endl;
    if (filesAllottedForMaple.find(failNode) == filesAllottedForMaple.end()) {
        return; 
    }
    
    auto m = mapleQ.front();

    thread sendMapleJobForFailThread(&mapleJuice::sendMapleJobForFailNode, this, m, failNode);
    sendMapleJobForFailThread.detach();  // let this run on its own    
}


void mapleJuice::handleMapleJobDone(int node) {
    filesAllottedForMaple.erase(node);

    if (filesAllottedForMaple.empty()) {
        lock_guard<mutex> lk(cvMapleJobDoneMutex);
        isAllMapleJobsDone = true;
        cout << "Notifying One for Maple Jobs Done" << endl;
        log() << "sdfs/ Notify other thread that all maple jobs are done";
        cvMapleJobDone.notify_one();
    }

}


void mapleJuice::handleJuiceJobDone(int node) {
    for (auto it = juiceIDs.begin(); it != juiceIDs.end(); it++) {
        if (it->second == node) {
            juiceIDs.erase(it);
            break;
        }
    }

    if (juiceIDs.empty()) {
        lock_guard<mutex> lk(cvJuiceJobDoneMutex);
        isAllJuiceJobsDone = true;
        cout << "Notifying One for Juice Jobs Done" << endl;
        log() << "sdfs/ Notify master thread that all juice jobs are done";
        cvJuiceJobDone.notify_one();
    }
}


void mapleJuice::handleJuiceJob(char* buf, int senderNode) {
    // parse to get: juice exe; juicerInputFile; sdfsSrcDirectory
    cout << "mapleJuice/ handleJuiceJob assigned by " << senderNode << endl;

    //setting up set for expect sources of juice input files
    for (int i=1; i < NODES+1; i++) {
        if (fs.ring[i]) {
            fs.juiceFilesNotifications.insert(i);
        }
    }

    juice j;
    int offset = 0;

    int juiceExeLen;
    memcpy(&juiceExeLen, buf+offset, sizeof(juiceExeLen));
    offset += sizeof(juiceExeLen);
    juiceExeLen = ntohl(juiceExeLen);

    j.juiceExe = string(buf+offset, juiceExeLen);
    offset += juiceExeLen;

    int numJuices;
    memcpy(&numJuices, buf+offset, sizeof(numJuices));
    offset += sizeof(numJuices);
    j.numJuices = ntohl(numJuices);

    int intermediateFileNameLen;
    memcpy(&intermediateFileNameLen, buf+offset, sizeof(intermediateFileNameLen));
    offset += sizeof(intermediateFileNameLen);
    intermediateFileNameLen = ntohl(intermediateFileNameLen);

    j.sdfsIntermediateFileNamePrefix = string(buf+offset, intermediateFileNameLen);
    offset+= intermediateFileNameLen;

    int sdfsDestFileNameLen;
    memcpy(&sdfsDestFileNameLen, buf+offset, sizeof(sdfsDestFileNameLen));
    offset += sizeof(sdfsDestFileNameLen);
    sdfsDestFileNameLen = ntohl(sdfsDestFileNameLen);

    j.sdfsDestFileName = string(buf+offset, sdfsDestFileNameLen);
    offset += sdfsDestFileNameLen;

    int myJuiceNumber;
    memcpy(&myJuiceNumber, buf+offset, sizeof(myJuiceNumber));
    myJuiceNumber = ntohl(myJuiceNumber);

    cout << "juice job recived with my number " << myJuiceNumber << endl;
    log() << "mapleJuice/ starting juice job thread for " << j.juiceExe;
    thread runJuiceJobThread(&mapleJuice::runJuiceJob, this, j, myJuiceNumber, senderNode);
    runJuiceJobThread.detach();  // let this run on its own
}


void mapleJuice::runJuiceJob(juice j, int myJuiceNumber, int master) {
    cout<< "fetching juice input files" << endl;
    log() << "mapleJuice/ fetching juice input files";
    // 1. fetch file to local
    fs.fetchJuiceInputFiles();
    cout << "all juice input files have been fetched" << endl;
    log() << "all juice input files have been fetched";

    string permCmd = "chmod 777 " + j.juiceExe;
    system (permCmd.c_str());

    string sysJuice = "./" + j.juiceExe + " ";
    string delCmd = "rm -f ";
    string outFileName = j.sdfsDestFileName + to_string(myJuiceNumber);
    ofstream outFile(outFileName, ios::app);
    cout << "starting to process juice files\n";
    for (auto it = fs.juiceFiles.begin(); it != fs.juiceFiles.end(); it++) {

        string juiceStr = sysJuice + *it;
        auto output = exec(juiceStr.c_str());
        outFile << output;
        
        string sysDel = delCmd + *it;
        system(sysDel.c_str());
    }
    cout << "all juice files processed\n";
    outFile.close();
    fs.juiceFiles.clear();

    //send file to master
    fs.pushFileToNode(master, outFileName, outFileName, "FILE");
    cout << "juice output sent to master\n";
    string cmd = "rm -f " + outFileName;
    system(cmd.c_str());
    log() << "mapleJuice/ juice task completed for " << j.juiceExe;
    cout << "mapleJuice/ juice task completed for " << j.juiceExe << endl;
    sendJuiceDoneMessage(master);
    cout << "job done sent to master\n";
}


void mapleJuice::sendJuiceDoneMessage(int node) {
    char message[10];
    strcpy(message, "JUID");
    int offset = 4;
    int connToServer;
    int status = connectToServer(node, &connToServer);
    if(status) {
        cout <<"sendJuiceJobDoneMessage: Cannot connect to node "<< node << endl;

    } else {
        log() << "mapleJuice/ sending juice job done message to " << node;
        write(connToServer, message, offset);
        close(connToServer);
    }
}


void mapleJuice::handleMapleJob(char* buf, int node) {
    maple m;
    int offset = 0;

    int mapleExeLen;
    memcpy(&mapleExeLen, buf+offset, sizeof(mapleExeLen));
    offset += sizeof(mapleExeLen);
    mapleExeLen = ntohl(mapleExeLen);

    m.mapleExe = string(buf+offset, mapleExeLen);
    offset += mapleExeLen;

    int intermediateFileNameLen;
    memcpy(&intermediateFileNameLen, buf+offset, sizeof(intermediateFileNameLen));
    offset += sizeof(intermediateFileNameLen);
    intermediateFileNameLen = ntohl(intermediateFileNameLen);

    m.sdfsIntermediateFileNamePrefix = string(buf+offset, intermediateFileNameLen);
    offset+= intermediateFileNameLen;

    int srcDirectoryLen;
    memcpy(&srcDirectoryLen, buf+offset, sizeof(srcDirectoryLen));
    offset += sizeof(srcDirectoryLen);
    srcDirectoryLen = ntohl(srcDirectoryLen);

    m.sdfsSrcDirectory = string(buf+offset, srcDirectoryLen);
    offset += srcDirectoryLen;

    int fileCount;
    memcpy(&fileCount, buf+offset, sizeof(fileCount));
    offset += sizeof(fileCount);
    fileCount = ntohl(fileCount);
    log() << "mapleJuice/ " << fileCount << " file names recvd for maple job " << m.mapleExe;

    for (int i=0; i < fileCount; i++) {
        int fileNameLen;
        memcpy(&fileNameLen, buf+offset, sizeof(fileNameLen));
        offset += sizeof(fileNameLen);
        fileNameLen = ntohl(fileNameLen);

        string fileName(buf+offset, fileNameLen);
        offset += fileNameLen;

        fs.mapleFiles.insert(fileName);
    }
    log() << "mapleJuice/ starting maple job thread for maple job " << m.mapleExe;
    thread runMapleJobThread(&mapleJuice::runMapleJob, this, m, node);
    runMapleJobThread.detach();  // let this run on its own
}


void mapleJuice::runMapleJob(maple m, int node) {
    for (auto it = fs.mapleFiles.begin(); it != fs.mapleFiles.end(); it++) {
        fs.fetchFile(*it, *it);
    }
    fs.recvMapleFiles();
    cout << "maple files recvd\n";
    for (auto it = fs.recvdMapleFiles.begin(); it !=fs.recvdMapleFiles.end(); it++) {
        cout << *it << endl;
    }
    log() << "mapleJuice/ all maple input files recvd for maple job " << m.mapleExe;

    string permCmd = "chmod 777 " + m.mapleExe;
    system (permCmd.c_str());

    string sysMap = "./" + m.mapleExe + " ";
    string outPrefix = m.sdfsIntermediateFileNamePrefix + "_" + to_string(fs.myNumber) + "_";

    for (auto it = fs.recvdMapleFiles.begin(); it != fs.recvdMapleFiles.end(); it++) {
        ifstream file(*it);
        if (!file.good()) {
            cout << "mapleJuice/ runMapleJob: can't open" << *it << endl;
            exit(1);
        }

        auto mapStr = sysMap + *it;
        int lines = 0;
        while (1) {
            auto cmdStr = mapStr + " " + to_string(lines);
            auto output = exec(cmdStr.c_str());
            if (output.size() == 0) {
                break;
            }
            istringstream iss(output);
            string line;
            while(getline(iss, line)) {
                string key;
                for (int i=0; i<line.size() && line[i]!='\t' ; i++) {
                    if (isalnum(line[i])) {
                        key += line[i];
                    }
                }
                //storing key value pair in file
                ofstream outfile(outPrefix + key, ios::app);

                if (!outfile.good()) {
                    cout << "mapleJuice/ runMapleJob: can't open" << outPrefix + key << endl;
                    exit(1);
                }
                outfile << line << endl;
                outfile.close();
                mapleOutFiles.insert(outPrefix+key);
                }
            
            lines += 10;
        }
    }
    fs.recvdMapleFiles.clear();
    unordered_set<string> deleteThese;
    storeMapleOutFiles(deleteThese);
    log() << "mapleJuice/ maple task completed for " << m.mapleExe;
    cout << "mapleJuice/ maple task completed for " << m.mapleExe << endl;
    sendMapleDoneMessage(node);
}


void mapleJuice::storeMapleOutFiles(unordered_set<string>& deleteThese) {
    bool flag = false;
    cout << "storing maple output files in SDFS" << endl;
    log() << "mapleJuice/ storing maple output files in SDFS";
    deleteThese.clear();
    for (auto it = mapleOutFiles.begin(); it != mapleOutFiles.end(); it++) {
        string fileName = *it;
        if (fs.storeFile(fileName, fileName)) {
            flag = true;
            break;
        }
        int node = fs.location(fileName);
        if (fs.myNumber != node && 
            fs.myNumber != fs.successorNode(node) && 
            fs.myNumber != fs.successorNode(fs.successorNode(node))) {
            deleteThese.insert(fileName);
        }
    }
    if (flag) {
        this_thread::sleep_for(chrono::milliseconds(3000));
        storeMapleOutFiles(deleteThese);
    }
    for (auto it = deleteThese.begin(); it != deleteThese.end(); it++) {
        string cmd = "rm -f " + *it;
        system(cmd.c_str());
    }
    deleteThese.clear();
}


void mapleJuice::sendMapleDoneMessage(int node) {
    char message[10];
    strcpy(message, "MAPD");
    int offset = 4;
    int connToServer;
    int status = connectToServer(node, &connToServer);
    if(status) {
        cout <<"sendMapleJobDoneMessage: Cannot connect to sender node "<< node << endl;

    } else {
        log() << "mapleJuice/ sending maple job done message to " << node;
        write(connToServer, message, offset);
        close(connToServer);
    }
}


void mapleJuice::master() {
    while(!mapleQ.empty()) {
        auto m = mapleQ.front();
        int nodes = fs.fd->list.size();
        m.numMaples = min(m.numMaples, nodes-1);

        log() << "mapleJuice/ asking nodes to send FileNames with prefix " << m.sdfsIntermediateFileNamePrefix;
        fs.getFileNames(m.sdfsSrcDirectory);
        log() << "mapleJuice/ All fileNames received";
        cout << "all FileName received" << endl;
        for (auto it=fs.fileNames.begin(); it != fs.fileNames.end(); it++) {
            cout << *it << endl;
        }
        int fileCount = fs.fileNames.size();
        m.numMaples = min(m.numMaples, fileCount);

        log() << "mapleJuice/ Sending maple jobs for " << m.mapleExe;
        cout << "Sending maple jobs for " << m.mapleExe << endl;

        isAllMapleJobsDone = false;
        sendMapleJobs(m);
        log() << "mapleJuice/ Maple jobs sent for " << m.mapleExe;
        cout << "Maple jobs sent for " << m.mapleExe << endl;

        // wait untill all maple jobs are done
        {
            unique_lock<mutex> lk(cvMapleJobDoneMutex);
            log() << "mapleJuice/ waiting for maple jobs to finish";
            cout << "Waiting for Maple jobs to finish... \n";
            cvMapleJobDone.wait(lk, [this]{return isAllMapleJobsDone;});
            log() << "mapleJuice/ all maple jobs are completed.";
            cout << "all maple jobs are completed\n";
        }

        mapleQ.pop();
        isJuiceTaskIssued = false;
        // wait if no juice job in the queue.
        if (juiceQ.empty()) {
            unique_lock<mutex> lk(cvWaitingForJuiceTaskMutex);
            log() << "mapleJuice/ waiting for juice task to be issued";
            cout << "Waiting for Juice job to be issued... \n";
            cvWaitingForJuiceTask.wait(lk, [this]{return isJuiceTaskIssued;});
        }
        auto j = juiceQ.front();
        j.numJuices = min(j.numJuices, nodes-1);

        cout << "starting juice job\n";
        isAllJuiceJobsDone = false;
        log() << "mapleJuice/ Sending juice jobs for " << j.juiceExe;
        cout << "Sending juice jobs for " << j.juiceExe << endl;


        sendJuiceJobs(j);
        cout << "all juice jobs sent" << endl;
        int node = fs.myNumber;
        for (int i=0; i<j.numJuices; i++) {
            node = fs.successorNode(node);
            juiceIDs[i] = node;
        }
        fs.sendJuiceFilesToJuicers(j.sdfsIntermediateFileNamePrefix, j.numJuices, juiceIDs);
        log() << "mapleJuice/ Juice jobs sent for " << j.juiceExe;
        cout << "Juice jobs sent for " << j.juiceExe << endl;
        
        // wait untill all juice jobs are done
        {
            unique_lock<mutex> lk(cvJuiceJobDoneMutex);
            log() << "mapleJuice/ waiting for juice jobs to finish";
            cout << "Waiting for juice jobs to finish... \n";
            cvJuiceJobDone.wait(lk, [this]{return isAllJuiceJobsDone;});
        }
        if (j.deleteInput) {
            fs.sendDeleteIntermediateFiles(j.sdfsIntermediateFileNamePrefix);
        }

        log() << "mapleJuice/ all juice jobs are completed.";
        cout << "all juice jobs are completed\n";
        collectJuiceFiles(j.sdfsDestFileName, j.numJuices);
        juiceQ.pop();
    }

    lock_guard<mutex> _(isMasterMutex);
    isMaster = false;
    cout << "all maple juice tasks are completed" << endl;
}


void mapleJuice::collectJuiceFiles(string fileName, int count) {
    vector<string> pairs;
    for (int i=0; i < count; i++) {
        ifstream inFile(fileName+to_string(i));
        string line;
        while(getline(inFile, line)) {
            pairs.push_back(line);
        }
        inFile.close();
        string cmd = "rm -f " + fileName + to_string(i);
        system(cmd.c_str());
    }
    sort(pairs.begin(), pairs.end());
    ofstream outFile(fileName);
    for (auto x: pairs) {
        outFile << x << endl;
    }
    outFile.close();
}


void mapleJuice::sendJuiceJobs(const juice& j) {
    int node = fs.myNumber;
    char message[MAXDATASIZE];
    strcpy(message, "JUIC");
    int offset = 4;

    int juiceExeLen = j.juiceExe.size();
    juiceExeLen = htonl(juiceExeLen);

    memcpy(message+offset, &juiceExeLen, sizeof(juiceExeLen));
    offset += sizeof(juiceExeLen);

    memcpy(message+offset, &j.juiceExe[0], j.juiceExe.size());
    offset += j.juiceExe.size();

    int numJuices = htonl(j.numJuices);
    memcpy(message+offset, &numJuices, sizeof(numJuices));
    offset += sizeof(numJuices);

    int intermediateFileNameLen = j.sdfsIntermediateFileNamePrefix.size();
    intermediateFileNameLen = htonl(intermediateFileNameLen);

    memcpy(message+offset, &intermediateFileNameLen, sizeof(intermediateFileNameLen));
    offset += sizeof(intermediateFileNameLen);

    memcpy(message+offset, &j.sdfsIntermediateFileNamePrefix[0], j.sdfsIntermediateFileNamePrefix.size());
    offset += j.sdfsIntermediateFileNamePrefix.size();

    int sdfsDestFileNameLen = j.sdfsDestFileName.size();
    sdfsDestFileNameLen = htonl(sdfsDestFileNameLen);

    memcpy(message+offset, &sdfsDestFileNameLen, sizeof(sdfsDestFileNameLen));
    offset += sizeof(sdfsDestFileNameLen);

    memcpy(message+offset, &j.sdfsDestFileName[0], j.sdfsDestFileName.size());
    offset += j.sdfsDestFileName.size();

    for (int i=0 ; i < j.numJuices; i++) {
        int innerOffset = offset;
        
        int yourNumber = htonl(i);
        memcpy(message+innerOffset, &yourNumber, sizeof(yourNumber));
        innerOffset += sizeof(yourNumber);

        node = fs.successorNode(node);
        // send juice exe to worker
        fs.pushFileToNode(node, j.juiceExe, j.juiceExe, "FILE");

        int connToServer;
        int status = connectToServer(node, &connToServer);
        if(status) {
            cout <<"sendMapleJobs: Cannot connect to "<< node << endl;

        } else {
            write(connToServer, message, innerOffset);
            close(connToServer);
        }
        log() << "mapleJuice/ juice task sent to " << node << " for " << j.juiceExe;
        juicerNumber.insert({node, i});
    }
}


int mapleJuice::getFreeNode() {
    
    int node = fs.myNumber;
    while(1) {
        node = fs.successorNode(node);
        auto it = filesAllottedForMaple.find(node);
        if (node != fs.myNumber && it == filesAllottedForMaple.end()) {
            return node;
        }
    }
}


void mapleJuice::sendMapleJobForFailNode(const maple &m, int failNode) {
    cout << "sending maple job for failed node " << failNode << endl; 
    int node = getFreeNode();
    
    char message[MAXDATASIZE];
    strcpy(message, "MAPL");
    int offset = 4;

    int mapleExeLen = m.mapleExe.size();
    mapleExeLen = htonl(mapleExeLen);

    memcpy(message+offset, &mapleExeLen, sizeof(mapleExeLen));
    offset += sizeof(mapleExeLen);

    memcpy(message+offset, &m.mapleExe[0], m.mapleExe.size());
    offset += m.mapleExe.size();

    int intermediateFileNameLen = m.sdfsIntermediateFileNamePrefix.size();
    intermediateFileNameLen = htonl(intermediateFileNameLen);

    memcpy(message+offset, &intermediateFileNameLen, sizeof(intermediateFileNameLen));
    offset += sizeof(intermediateFileNameLen);

    memcpy(message+offset, &m.sdfsIntermediateFileNamePrefix[0], m.sdfsIntermediateFileNamePrefix.size());
    offset += m.sdfsIntermediateFileNamePrefix.size();

    int srcDirectoryLen = m.sdfsSrcDirectory.size();
    srcDirectoryLen = htonl(srcDirectoryLen);

    memcpy(message+offset, &srcDirectoryLen, sizeof(srcDirectoryLen));
    offset += sizeof(srcDirectoryLen);

    memcpy(message+offset, &m.sdfsSrcDirectory[0], m.sdfsSrcDirectory.size());
    offset += m.sdfsSrcDirectory.size();

    int fileCount = 0;
    int rOffset = offset;
    memcpy(message+offset, &fileCount, sizeof(fileCount));
    offset += sizeof(fileCount);

    auto fileRange = filesAllottedForMaple[failNode];
    auto it = fs.fileNames.begin();
    
    while(fileRange.first != *it) {
        it++;
    }
    int i = 0;
    while(*it != fileRange.second) {
        string fileName = *it;
        int fileNameLen = fileName.size();
        fileNameLen = htonl(fileNameLen);

        memcpy(message+offset, &fileNameLen, sizeof(fileNameLen));
        offset += sizeof(fileNameLen);

        memcpy(message+offset, &fileName[0], fileName.size());
        offset += fileName.size();
        it++;
        i++;
    }

    string fileName = *it;
    int fileNameLen = fileName.size();
    fileNameLen = htonl(fileNameLen);

    memcpy(message+offset, &fileNameLen, sizeof(fileNameLen));
    offset += sizeof(fileNameLen);

    memcpy(message+offset, &fileName[0], fileName.size());
    offset += fileName.size();
    i++;

    fileCount = htonl(i);
    memcpy(message+rOffset, &fileCount, sizeof(fileCount));

    // send maple exe to worker
    fs.pushFileToNode(node, m.mapleExe, m.mapleExe, "FILE");

    int connToServer;
    int status = connectToServer(node, &connToServer);
    if(status) {
        cout <<"sendMapleJobs: Cannot connect to "<< node << endl;

    } else {
        write(connToServer, message, offset);
        close(connToServer);
    }
    log() << "mapleJuice/ maple task sent to " << node << " for " << m.mapleExe;
    log(DEBUG) << "mapleJuice/ "<< fileCount << " fileNames sent to " << node;
    filesAllottedForMaple.erase(failNode);
    filesAllottedForMaple.insert({node, fileRange});
    cout << "maple job for fail node " << failNode << " sent to " << node << endl;
}


void mapleJuice::sendMapleJobs(const maple& m) {

    int qout = fs.fileNames.size() / m.numMaples;
    int rem = fs.fileNames.size() % m.numMaples;
    cout << "quot = " << qout << ", rem = " << rem << endl;

    auto it = fs.fileNames.begin();

    int node = fs.myNumber;
    for (int i=0; i < m.numMaples; i++) {
        node = fs.successorNode(node);

        char message[MAXDATASIZE];
        strcpy(message, "MAPL");
        int offset = 4;

        int mapleExeLen = m.mapleExe.size();
        mapleExeLen = htonl(mapleExeLen);

        memcpy(message+offset, &mapleExeLen, sizeof(mapleExeLen));
        offset += sizeof(mapleExeLen);

        memcpy(message+offset, &m.mapleExe[0], m.mapleExe.size());
        offset += m.mapleExe.size();

        int intermediateFileNameLen = m.sdfsIntermediateFileNamePrefix.size();
        intermediateFileNameLen = htonl(intermediateFileNameLen);

        memcpy(message+offset, &intermediateFileNameLen, sizeof(intermediateFileNameLen));
        offset += sizeof(intermediateFileNameLen);

        memcpy(message+offset, &m.sdfsIntermediateFileNamePrefix[0], m.sdfsIntermediateFileNamePrefix.size());
        offset += m.sdfsIntermediateFileNamePrefix.size();

        int srcDirectoryLen = m.sdfsSrcDirectory.size();
        srcDirectoryLen = htonl(srcDirectoryLen);

        memcpy(message+offset, &srcDirectoryLen, sizeof(srcDirectoryLen));
        offset += sizeof(srcDirectoryLen);

        memcpy(message+offset, &m.sdfsSrcDirectory[0], m.sdfsSrcDirectory.size());
        offset += m.sdfsSrcDirectory.size();

        int fileCount = qout;
        if (rem > 0) {
            fileCount++;
            rem--;
        }
        int fileCountSent = htonl(fileCount);

        memcpy(message+offset, &fileCountSent, sizeof(fileCountSent));
        offset += sizeof(fileCountSent);

        pair<string, string> fileRange;

        fileRange.first = *it;

        for (int j=0; j < fileCount && it != fs.fileNames.end() ; j++) {
            string fileName = *it;
            int fileNameLen = fileName.size();
            fileNameLen = htonl(fileNameLen);

            memcpy(message+offset, &fileNameLen, sizeof(fileNameLen));
            offset += sizeof(fileNameLen);

            memcpy(message+offset, &fileName[0], fileName.size());
            offset += fileName.size();

            fileRange.second = fileName;
            it++;
        }

        // send maple exe to worker
        fs.pushFileToNode(node, m.mapleExe, m.mapleExe, "FILE");

        int connToServer;
        int status = connectToServer(node, &connToServer);
        if(status) {
            cout <<"sendMapleJobs: Cannot connect to "<< node << endl;

        } else {
            write(connToServer, message, offset);
            close(connToServer);
        }
            log() << "mapleJuice/ maple task sent to " << node << " for " << m.mapleExe;
            log(DEBUG) << "mapleJuice/ "<< fileCount << " fileNames sent to " << node;
            filesAllottedForMaple.insert({node, fileRange});
    }
}


void mapleJuice::handleInput() {
    // handleInput for both failure_detector and sdfs
    string input;
    while (1) {
        cin >> input;
        if (input.compare("join") == 0) {
            int otherNode;
            cin >> otherNode;
            fs.fd->sendJOIN(otherNode);

        } else if (input.compare("id") == 0) {
            cout << fs.fd->getBirthTime() << endl;

        } else if (input.compare("list") == 0) {
            fs.fd->printList();

        } else if (input.compare("leave") == 0) {
            fs.fd->leave();
            log(INFO) << "Leaving the system.";
            exit(1);

        } else if (input.compare("put") == 0) {
            string localFileName, sdfsFileName;
            cin >> localFileName >> sdfsFileName;
            fs.storeFile(localFileName, sdfsFileName);

        } else if (input.compare("get") == 0) {
            string localFileName, sdfsFileName;
            cin >> sdfsFileName >> localFileName;
            fs.fetchFile(sdfsFileName, localFileName);

        } else if (input.compare("delete") == 0) {
            string fileName;
            cin >> fileName;
            fs.deleteFile(fileName);

        } else if (input.compare("store") == 0) {
            fs.showStore();

        } else if (input.compare("ls") == 0) {
            string fileName;
            cin >> fileName;

            fs.showFileLocations(fileName);

        } else if (input.compare("ring") == 0) {
            fs.printRing();

        } else if (input.compare("next") == 0) {
            cout << fs.successorNode(fs.myNumber) << endl;

        } else if (input.compare("maple") == 0) {
            maple m;
            cin >> m.mapleExe >> m.numMaples >> m.sdfsIntermediateFileNamePrefix >> m.sdfsSrcDirectory;
            mapleQ.push(m);
            lock_guard<mutex> lk(isMasterMutex);
            if (!isMaster) {
                isMaster = true;
                thread masterThread(&mapleJuice::master, this);
                masterThread.detach();  // let this run on its own
            }

        } else if (input.compare("juice") == 0) {
            juice j;
            cin >> j.juiceExe >> j.numJuices >> j.sdfsIntermediateFileNamePrefix
                >> j.sdfsDestFileName >> j.deleteInput;
            if (juiceQ.empty()) {
                juiceQ.push(j);
                lock_guard<mutex> lk(cvWaitingForJuiceTaskMutex);
                isJuiceTaskIssued = true;
                log() << "sdfs/ Notify master thread that a juice task has been issued";
                cvWaitingForJuiceTask.notify_one();
            } else {
                juiceQ.push(j);
            }

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


void mapleJuice::createSocket() {
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
    serv_addr.sin_port = htons(PORT3);

    log(INFO) << "Binding socket.";
    if (bind(sockFd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        perror("Cannot bind");
        close(sockFd);
    }
    log(INFO) << "Socket binding done.";
}


int mapleJuice::connectToServer(int targetNode, int *connectionFd) {
    struct in_addr tmp;
    tmp.s_addr = htonl(fs.fd->IPAddrs[targetNode]);
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
    auto portStr = to_string(PORT3);
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

