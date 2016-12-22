/*
 * @file util.cc
 * @author Shahzad Bhatti
 * @date Sep 29, 2016
 *
 */
#include "util.h"
#include <cstdlib>
#include <iostream>

// for hash_str
#define A 54059 /* a prime */
#define B 76963 /* another prime */
#define C 86969 /* yet another prime */
#define FIRSTH 37 /* also prime */

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

unsigned hash_str(const char* s) {
   unsigned h = FIRSTH;
   while (*s) {
     h = (h * A) ^ (s[0] * B);
     s++;
   }
   return h; // or return h%C
}

void partition_string(string str, char c, string& str1, string& str2) {
    string::size_type pos;
    pos = str.find(':',0);
    str1 = str.substr(0,pos);
    str2 = str.substr(pos+1);
}

bool replace(std::string& str, const std::string& from, const std::string& to) {
    size_t start_pos = str.find(from);
    if(start_pos == std::string::npos)
        return false;
    str.replace(start_pos, from.length(), to);
    return true;
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
/*
string parseFileName(string fileName) {
    string name;
    int i;
    int fileNameSize = fileName.size();
    for (i=0; i < fileNameSize; i++) {
        if(fileName[i] == '_') {
            i++;
            break;
        }
        name += fileName[i];
    }
    for (; i < fileNameSize; i++) {
        if (fileName[i] == '_')
        {
            break;
        }
    }
    if (i > fileNameSize - 1) {
        return string();
    }
    for (; i < fileNameSize; i++) {
        name += fileName[i];
    }
    return name;
}
*/

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

