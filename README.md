## Make ane Run the program

This program can be used to create a simple distributed file system. It also detects failures of nodes in a distributed system and replicates the files appropriately. Every file is replicated on three different nodes. It guarantees completeness for upto two consective failures under the assumption that we have at three live nodes at all times.

It also allows to grep on log files from different machines in the system.

## Make 
To make the program run "make"

## Running failure detector
* run the program as ./node <vm number>. Note, vm number is needed to get the IP address. You would have format of machine names in ffailure_detector/failure_detector.{cc,h} and node.cc
* To join a node to the existing system, give the command "join <vm number>" where <vm number> is the number of an existing node.
* To see the id of the node, give the command "id". id is the birthTime of a node in microseconds.
* To see current membership list, give command "list". list shows id and ip addresses of current nodes.
* To make a node leave the system, give the command "leave". 
* To put a file in the system, give the command "put <local filename> <sdfs filename>"
* To get a file from the system, give the command "get <sdfs filename> <local filename>"
* To delete a file from the system, give the command "delete <sdfs filename>"
* To see the files store on a node, give the command "store"
* To list the nodes replicating a file, give the command "ls <sdfs filename>"

## Running distributed grep on log files
* Run ./send-log on all the machine where log files are located.
* On one of the machines run ./query-log <grep options> <grep string>.


