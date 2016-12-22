/*
 * @file util.cc
 * @author Shahzad Bhatti
 * @date Sep 29, 2016
 *
 */
#include "util.h"
#include <cstdlib>
#include <iostream>


uint64_t htonll(uint64_t host_longlong) {
    int x = 1;
    /* little endian */
    if(*(char *)&x == 1)
        return ((((uint64_t)htonl(host_longlong)) << 32) + htonl(host_longlong >> 32));
    /* big endian */
    else
        return host_longlong;
}


uint64_t ntohll(uint64_t host_longlong) {
    int x = 1;
    /* little endian */
    if(*(char *)&x == 1)
        return ((((uint64_t)ntohl(host_longlong)) << 32) + ntohl(host_longlong >> 32));
    /* big endian */
    else
        return host_longlong;
}


uint64_t timeNow() {
    struct timeval current;
    gettimeofday(&current, 0);
    return current.tv_sec * 1000 * 1000 + current.tv_usec;
}


bool isPrefix(const string& prefix, const string& str) {
    auto res = std::mismatch(prefix.begin(), prefix.end(), str.begin());
    return res.first == prefix.end();
}


bool fileExists(const std::string& name) {
    ifstream f(name.c_str());
    return f.good();
}


string exec(const char* cmd) {
    char buffer[2000];
    string result;
    shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) {
        cout << "could not open pipe" << endl;
        exit(1);
    }
    while (!feof(pipe.get())) {
        if (fgets(buffer, 1000, pipe.get()) != NULL) {
            result += buffer; 
        }
    }
    return result;
}


string getKey(string fileName) {
    string key;
    int i;
    int fileNameSize = fileName.size();
    for (i=0; i < fileNameSize; i++) {
        if(fileName[i] == '_') {
            i++;
            break;
        }
    }
    
    if (i >= fileNameSize - 1) {
        return string();
    }
    
    for (; i < fileNameSize; i++) {
        if (fileName[i] == '_')
        {
            i++;
            break;
        }
    }
    if (i >= fileNameSize) {
        return string();
    }
    for (; i < fileNameSize; i++) {
        key += fileName[i];
    }
    return key;
}

