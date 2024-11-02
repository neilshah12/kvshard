# B2

Random load balancing assumes that all nodes within a shard are always homogeneous; that is, it assumes all nodes have equal capability to handle requests, which is not necessarily true. For example, some nodes with higher CPU resources will likely be able to handle more requests, so a uniform load balacing is not ideal, because this underutilizes CPU. 

Similarly, if a node has more shards assigned to it, it may already be handling more load than other nodes with fewer shards assigned. In this case, random load balancing may exacerbate the load imbalance.

# B3

Trying every node until one succeeds might increase the burden on the system, especially if a given shard has many nodes. This might reduce response times for requests that would otherwise succeed on the first try. For instance, if a node is already overloaded and receives a request as a result of the retry mechanism, and then receives another "new" request from a client, it will prioritize the retry request over the new request. The new request will wait in a queue until the retry request is processed, which will increase the response time for the new request. To address this issue, you could implement a more sophisticated load balancing strategy that takes into account the load on each node and assigns requests to nodes accordingly. 

Or, you could implement a priority queue (with a timeout for retries) to ensure that new requests are prioritized over retry attempts. 