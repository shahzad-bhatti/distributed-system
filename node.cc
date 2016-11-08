/*
 * @file node.cc
 * @author Shahzad Bhatti
 * @date Sep 28, 2016
 *
 */

#include "failure_detector/failure_detector.h"
#include "sdfs/sdfs.h"
#include "logger/logger.h"
#include "sdfs/sdfs.h"
// #include "push.cpp"

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

    sdfs fs(number, fileName, INFO);

    thread inputThread(&sdfs::handleInput, &fs);

    fs.recvMessages();

    return 0;
}
