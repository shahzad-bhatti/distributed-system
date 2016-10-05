:## Make ane Run the program

This program can be used to detect failures of nodes in a distributed system. It also allows to grep on log files from different machines in the system.

## Make 
To make the program run "make"

## Running failure detector
* run the program as ./node <vm number>. Note, vm number is needed to get the IP address. You would have format of machine names in ffailure_detector/failure_detector.{cc,h} and node.cc
* To join a node to the existing system, give the command "join <vm number>" where <vm number> is the number of an existing node.
* To see the id of the node, give the command "id". id is the birthTime of a node in microseconds.
* To see current membership list, give command "list". list shows id and ip addresses of current nodes.
* To make a node leave the system, give the command "leave". 

## Running distributed grep on log files
* Run ./send-log on all the machine where log files are located.
* On one of the machines run ./query-log <grep options> <grep string>.


