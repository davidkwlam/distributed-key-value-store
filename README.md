# Distributed Key-Value Store

A distributed key-value store that implements quorum consensus replication and read-repair.

1.) Overall design

This system is comprised of two main components: the coordinator and the store. Coordinators handle client requests and stores handle routed requests from coordinators. Each node is both a coordinator and a store.

Coordinator:

The coordinator accepts requests from clients and forwards them to the store on the three replicas responsible for the request’s key. It is comprised of:

Coordinator.java
Monitor.java
Node.java

The coordinator class listens for client requests and routes them to the replicas. The monitor class keeps track of node joins and failures, as well as providing the routing information for the coordinator. The node class is used to keep track of the current status of a node.

Store:

The store is a simple wrapper class around a ConcurrentHashMap that keeps track of stores key-value pairs along with the version number. Stores process requests from coordinators. It is comprised of:

Store.java
StoreClient.java
StoreMessage.java

Coordinators forward requests to stores using the StoreClient class via UDP messaging. These messages are the same as the messages sent from clients to the coordinator except they include an extra field for the key-value version. The StoreMessage class is made up of helper methods for creating these messages.
 
2.) What happens during normal operation?

On start, the Server class reads in a file containing a list of n nodes. Each node will be put into an array of size n. The index of a node in the array represents the ID of the node. Keys are hashed using SHA-512 and the result modded by the number of nodes to get the id of the node responsible for that key. Since there is a replication factor of three, the assigned node plus its two successor nodes are responsible for a given key.

Write (PUT/REMOVE) request results are returned to the client when a majority of stores (WRITE_QUORUM) return the same response to the coordinator. Each write increments the local version number of a key-value at each store.

Read (GET) requests are returned to the client when a READ_QUORUM number of stores return a result to the coordinator. The value with the greatest version number is returned to the  client. In this case, READ_QUORUM = REPLICATION_FACTOR - WRITE_QUORUM + 1. Note that for a replication factor of 3, both WRITE_QUORUM and READ_QUORUM equal 2. Replicas with older versions of the key-value pair are repaired after a GET result is returned to the client (i.e. eventual consistency is achieved via read-repair).

3.) What happens after a node failure (and how does it get detected)?

The coordinator monitors node fails two ways: by observing failed requests and by periodically requesting a heartbeat from the store located on every known node.

Failed nodes are marked as failed and subsequent requests involving that node are routed to the next available node.

4.) What happens after a node join?

Since the coordinator requests heartbeats from node stores regardless of whether they are up and down, when a node joins it will begin responding to heartbeat requests. When a heartbeat is successfully returned to the requestor, the requestor will mark that node as available. 

5.) Performance optimizations

The main optimization comes from utilizing a thread pool (ExecutionService) to execute asynchronous tasks. There are three dedicated threads for receiving client requests, forwarded requests, and returned responses from stores. All other actions are non-blocking and executed via a thread pool, minimizing the total overhead needed for creating and switching between threads.

Since each request needs only two responses from stores before returning to a client (and all forwarded requests are executed in parallel), each request involves two round-trips, where the total response time is dictated by the the time it takes to return a response from the second fastest node.
