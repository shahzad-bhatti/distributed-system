This program can be used to create a simple distributed file system. It also detects failures of nodes in a distributed system and replicates the files appropriately. Every file is replicated on three different nodes. It guarantees completeness for upto two consective failures under the assumption that we have at three live nodes at all times.

A mapReduce job can be run via any node. This node becomes master for the mapReduce task and assigns map task to other nodes. Once the map task is completed by all nodes, the master then assigns reduce task to other nodes. Map and reduce tasks can be issued via the following commands.
```sh
maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
```
```sh
juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
```
The system also allows to grep on log files from different machines in the system.

## Make 
To make the program run "make"

## Run
* run the program as ``./node <vm number>``. Note, vm number is needed to get the IP address. You would have to format machine names in ``failure_detector/failure_detector.cc, sdfs/sdfs.cc and mapleJuice/mapleJuice.cc``.

## Interacting with the system
* To join a node to the existing system, give the command ``join <vm number>`` where ``<vm number>`` is the number of a node already present in the distributed system.
* To see the id of the node, give the command "id". id is the birthTime of a node in microsecond
* To see current membership list, give command "list". list shows id and ip addresses of current nodes.
* To make a node leave the system, give the command "leave". 
* To put a file in the system, give the command put <local_filename> <sdfs_filename>"
* To get a file from the system, give the command "get <sdfs_filename> <local_filename>"
* To delete a file from the system, give the command "delete <sdfs_filename>"
* To see the files store on a node, give the command "store"
* To list the nodes replicating a file, give the command "ls <sdfs_filename>"

## Running distributed grep on log files
* Run ./send-log on all the machine where log files are located.
* On one of the machines run ./query-log <grep options> <grep string>.


