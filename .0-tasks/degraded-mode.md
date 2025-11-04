We need a new "degraded mode" variable for the node service.
When the variable is set to true, the node service will not serve traffic and register itself in the cluster.
We can call the node to have it load snapshots for example but it should not serve traffic.
The nodes that are not in degraded mode will know which nodes are in degraded mode and will not advertise those
node addresses to the clients during the usual GET and PUT requests.