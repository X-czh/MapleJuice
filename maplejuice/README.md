# MapleJuice

MapleJuice is a batch processing system that works like MapReduce.

## Programming Model

MapleJuice consists of two phases of computation – Maple (brother of Map) and Juice (sister of Reduce). Maple takes a set of data and transforms their individual lines into a list of key-value pairs. Juice combines the output from Maple into a smaller list of key-value pairs. However, the Maple function processes 10 input lines from a file simultaneously at a time, while the traditional Map function processed only one input line at a time. The Maple and Juice tasks are user-defined. Users can upload any Maple/Juice executable files that fit the programming model to the system. For simplicity, our system can only run one phase at a time.

## System Architecture

MapleJuice is composed of three major components: i) clients for interacting with users, ii) single master node for task scheduling and coordination, iii) worker nodes for task processing. It uses our Distributed Group Membership Service in MP2 for failure detection and Simple Distributed File System (SDFS) in MP3 for storing the input data and the results of the MapleJuice jobs.

### Workflow

Users submit jobs on clients, which then send job requests to the master node. When doing a Maple job, the master node partitions the input data and evenly distributes the partitioned data to a set of selected workers. Each worker processes its share of data by repeatedly calling the user-uploaded Maple executable. The generated key-value pairs are sent back to the master to be gathered and written to SDFS, one file per key. When doing a subsequent Juice job, the master node shuffles the keys and allocates them to another set of selected workers. The workers process their dispatched task by repeatedly calling the user-uploaded Juice executable. The processed results are sent back to the master to be combined and written to SDFS.

### Scheduling

The master node is in charge of all the scheduling work. It maintains first-in, first-out job queue, allowing at most one job to run at a time with all subsequent jobs waiting in queue. When dispatching tasks to workers, it prefers worker nodes on which replicas of input files already exist to reduce file I/O. When doing failure recovery, it favors free workers (if any) to restart the task previously running on the failed worker.

### Fault Tolerance

MapleJuice can tolerate up to 3 simultaneous worker failures, limited by the replication factor (4) used in SDFS. A master failure cannot be tolerated though. On the master node, a worker-task mapping table is maintained. When a working worker is reported as failed, the master node retrieves the information of its running task(s), and selects another worker (free worker preferred) to restart the task(s).
