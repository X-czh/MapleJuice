# Distributed Group Membership Service

Our distributed group membership service places each member in the group on an extended virtual ring sorted by their IDs. An member's ID consists of its IP address and join time. Each member keeps a complete group membership list but can only directly communicate with some of its close neighbors on the ring.

## Join and leave

There is one fixed contact process that acts as the introducer and all potential members know about. When a new member wants to join the group, it sends a join request to the introducer, which then sends the current membership list back and disseminates the new node join information. During the time the introducer is down, no new member can join the group. When a member wants to leave the group, it disseminates its leave information. Each member will update its membership list upon receiving a join/leave message.

## Failure detection

Each member in the group sends one heartbeat message per second to its three closest successors on the ring. A member is marked as failed and removed from the membership list if any of its heartbeat receiver fails to receive its heartbeat message for 4 s. A node fail message will be disseminated and each member will update its membership list upon receiving a node fail message. As there can be at most three simultaneous failures in the group, if a node fails, its three heartbeat receivers cannot all fail, and its failure must be reported. Furthermore, assuming synchronous clock in the group, it is guaranteed that a machine failure must be reflected within 5 s, for the maximum possible delay of detecting a node's failure takes place when a node fails as soon as it acks a ping, and it takes at most 1 s before the next ping to it is sent, and another 4 s to wait for timeout, which is a total of 5 s. We can also decrease the timeout to achieve a faster fail detection, while the trade-off is higher false positive rate in a lossy network.

## Introducer election

When the introducer leaves or fails, a new introducer will be elected. Since our system is operated on virtual machines cluster provided by the university, we assume that the network is synchronous. The election is done via using the Bully algorithm.

## Dissemination of messages

The membership update (join/leave/fail) messages are sent to its four closest successors. If a successor finds the message already received, it stops relaying it, otherwise it further sends the message to its four closest successors. By using our protocol for a ten-node group, it takes only 3 rounds of relaying to have one message sent from a node received by all nodes, and it is guaranteed that node failure can be reflected in at least one membership list within 5 s regardless of network latency. Assuming small network latency, a membership update can be reflected within 6 s at all membership lists at high probability.

## Marshaled message format

Our message are all encoded in UTF-8 string, which has a fixed byte order and is endianness independent. Each message consists of a message type (join/leave/fail/ping/ack), and a payload. For join/leave/fail, the payload is just the id of the newly joined/left/failed node. For ping/ack, the payload is empty string.

## Scalability

The design can easily be scaled up to large N. In our protocol, every node only sends heartbeat to a fixed number of targets. During dissemination of any type of message, each node only forwards the message to a fixed number of its neighbors. Such design avoids the issue of single point failure of multicast or broadcast. In addition, it also solves the problem of message load explosion of all-to-all communication by distributing the message load evenly to every node.
