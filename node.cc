/*
 * @file node.cc
 * @author Shahzad Bhatti
 * @date Sep 28, 2016
 *
 */

#include "failure_detector/failure_detector.h"
#include "logger/logger.h"

#include <iostream>
#include <string>
#include <thread>


using namespace std;

int main(int argc, char *argv[]) {
    
    if (argc <2) {
        cout << "USAGE: " << argv[0] << " <my VM number> " << endl; 
        return 0;
    }
    int number = atoi(argv[1]);
    
    string fileName = "machine.";
    if (number < 10) {
        fileName += "0";
        fileName += to_string(number);
    } else {
        fileName += to_string(number);
    }
    fileName += ".log";
    failureDetector fd(number, fileName, INFO);
    
    thread inputThread(&failureDetector::handleInput, &fd);   
    thread pThread(&failureDetector::sendPING, &fd);   
    fd.recvMessages();
    
    return 0;
}
