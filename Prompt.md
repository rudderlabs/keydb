# KeyDB

KeyDB is a Key store (not a KV store) designed to be fast and scalable and eventually consistent.

## Summary

KeyDB is a distributed system written in Golang that follows the Effective Go best-practices and the SOLID principles 
that will work as a distributed KV store with TTL support.
Code quality and test coverage are of the utmost importance. 
All scenarios need to be covered. The repository will include unit tests but also tests that will create a cluster of 
multiple nodes and have them communicate with each other so that we can test that all our requirements are met.

## Requirements

Data is kept in-memory in a data structure made of a map plus a double linked-list sorted by TTL.
The map value will be the node of the doubled linked-list.

For such data structure this library will be used: https://github.com/rudderlabs/rudder-go-kit/blob/main/cachettl/cachettl.go

The `cachettl` library will be configured with `config.refreshTTL=false` so that the TTL is not refreshed each time we call `Get`.

* Being a distributed system each node will have a number assigned, and it will know how many nodes the cluster is comprised of.
  This will allow the nodes to know which hash ranges they are serving.
  The number of nodes will initially be set via an environment variable, but it can be superseded by the Operator
  (explained later) during `SCALE` operations via gRPC.
* Being a distributed system each node will handle only a set of hash ranges. There are going to be X hash ranges in
  total (default 128). Each node will know which hash ranges it will handle at a given time by knowing its own node 
  number and how many nodes the cluster is comprised of. The hashing functions need to be deterministic and as efficient
  as possible. We will need two hashing functions:
  1. one function that given the number of nodes in the cluster and a node index it will tell us which
     hash ranges the node with that index serves. 
  2. one function that given a key it will tell you in which hash range it falls into. this function combined with the
     other function above (bullet point 1) we should be able to handle all cases in the nodes, Client and Operator (explained later).
* Every X seconds (configurable, default 60s) each node will autonomously create one snapshot per hash range that is handling
  on disk using a custom data format to save the content of the double linked list on disk sorted by TTL so that upon 
  recovering we can read the file and repopulate the map+double linked list. These files would be called snapshots from 
  now on, and each snapshot needs to written into a temp file first and then replace the actual snapshot with a "rename" 
  operation in order to be atomic (this is to avoid partial writes). Each line in the snapshot file is a node of the 
  double linked list with key and timestamp, nothing else. The file format needs to be as small as possible, use the 
  position of the fields to determine the fields values. Being the double linked-list already sorted by TTL the snapshot 
  will include the sooner to expire nodes first. When loading the snapshot, nodes that are already expired can be discarded. 
  The same when creating snapshots, expired nodes don't need to be included in snapshots. 
  * if these criteria cannot be met with the rudder-go-kit library we can create a local copy of the rudder-go-kit 
    library in this repo and modify it as needed.
* Each node will be connected to each other via gRPC and transfer the changed snapshots when they are updated
  (default 60s as per bullet point above). This way all nodes will have all snapshots about all hash ranges always 
  available on disk although they might be outdated at times. The nodes will only keep in-memory a single data 
  structure (cachettl) with all the hash ranges that it needs to handle given its node number. For example if we have a 
  total of 10 hash ranges and the cluster is made of 5 nodes then each node might handle 2 hash ranges. Then given the
  example each node will have all 10 hash ranges snapshots on disk but one data structure in memory with the data of its 
  own 2 hash ranges, not the other 8.
* The scaling up and down will be done manually by using a Go `Operator` client that will live in the same codebase. 
  Such client will use gRPC to call all the nodes and instruct them whether a scale up or a scale down is needed.
  The `Operator` client will first send a CREATE SNAPSHOT command and wait for all nodes to create a snapshot by checking 
  their response. Then the `Operator` client sends a SCALE command to tell the nodes how many nodes are now in the cluster.
  As a result they nodes will try to get the snapshots of the new hash-ranges that they will have to handle from a 
  neighbour and they will load them memory replacing what they had.
* In the same codebase we will also have a Go gRPC client that will be used to communicate with the nodes to send GET 
  requests and PUT requests used to interact with the actual KV store that we are designing. We will call it `Client`.
  The nodes will always advertise in their responses how many nodes the cluster is comprised of.
  The `Client` will use such information to know which node it should call for a given key. In fact the `Client` will be 
  aware of the same hashing functions as the nodes so that given a key and the number of nodes it will know which node 
  it should send the request to.
* The Go gRPC `Client` and the Go gRPC `Operator` will both live in the cmd folder, one in "client" and one in the 
  "operator" folder.
* You will have to use the latest Go version 1.24.3 and containerized the nodes in a Dockerfile that uses the latest 
  Alpine and Go versions. We don't need containers for the two clients (`Client` and `Operator`). Make sure to create a 
  proper `go.mod` as well.
* Only one of `CREATING SNAPSHOT` and `SCALE` command can happen at a given time, they cannot happen simultaneously.
  * Consider that `CREATING SNAPSHOT` can happen for two reasons. One because the `Operator` triggered it, and one because
    each node creates the snapshots of the hash ranges it handles autonomously every X seconds (configurable as explained
    above).
