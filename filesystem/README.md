# Simple Distributed File System

Our simple distributed file system (SDFS) provides fault-tolerant distributed file system services with fast time-bounded write-write conflicts. It is built on top of our distributed group membership service, where each member in the group is placed on an extended virtual ring sorted by their IDs.

## Replication

We use a replication factor (RF) of 4 to tolerate up to three machine failures at a time. We do not shard the files and always replicate an entire file. There is no master node involved. Each node stores the full file-to-machine mapping table and directly contacts the machines with the file to perform read/write. A quorum-based consensus method is used to provide fast response time but still ensures strong consistency. We use a read replica count (R) of 1 and a write replica count (W) of 4, so that W + R $>$ RF and W $>$ RF/2. When a node leaves the group or fails, the distributed group membership service will notify the SDFS. For each file stored on the left/failed node, the node with the smallest id among the nodes storing the file will be selected to carry out re-replication on randomly selected nodes without the file. The random selection of nodes can reduce the chance of having uneven file storage distribution.

## Time-bounded write-write conflicts

A modified version of Maekawa's algorithm is used to provide time-bounded write-write conflicts. Each write operation will broadcast to request write permission from each node. If a node finds that the last write to the same file (if exists) does not take place within a minute ago, it grants permission to the request and updates the last write time, otherwise it rejects the request. If a write operation receives a quorum of permissions, it is approved, otherwise confirmation from the user is required. If no confirmation is received within 30 s, the write operation is automatically rejected.
