/*
 * @file node.cc
 * @author Shahzad Bhatti
 * @date Sep 28, 2016
 *
 */

#include "mapleJuice/mapleJuice.h"
#include "failure_detector/failure_detector.h"
#include "sdfs/sdfs.h"
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

    logger log(fileName);
    log.setLevel(INFO);
    mapleJuice mj(number, log);

    thread inputThread(&mapleJuice::handleInput, &mj);

    mj.recvMessages();

    return 0;
}
