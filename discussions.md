# A4

The TTL expiration cleanup runs on a fixed interval (every 5 seconds), which means expired keys are only removed periodically, not immediately upon expiration. The tradeoffs of this approach are that there will be a set amount of time where unnecessary storage is used for expired keys, leading to the possibility of failed gets. The cleanup operation iterates over all keys across all shards during each cleanup cycle. This reduces the need for per-key management overhead but can become costly as the number of keys grows, especially if only a small subset of keys has expired. The cost of each cleanup cycle is proportional to the number of keys in each shard. As the number of keys increases, the cleanup cost rises since the process must iterate over all keys to identify expired entries. If the server has many keys with varying TTLs, this could become expensive.

If the server was write-intensive, the current TTL cleanup strategy could cause contention between cleanup and write operations, as each cleanup cycle acquires locks on shards to delete expired keys. If I knew it was write-intensive, I think I would simply clean up less often, or have a more sophisticated way of cleaning up as opposed to just iterating thorugh everythign and holding the lock. You could do lazy expiration on access, where you expire keys only when you realize that they are expired.

# B2

Random load balancing assumes that all nodes within a shard are always homogeneous; that is, it assumes all nodes have equal capability to handle requests, which is not necessarily true. For example, some nodes with higher CPU resources will likely be able to handle more requests, so a uniform load balacing is not ideal, because this underutilizes CPU. 

Similarly, if a node has more shards assigned to it, it may already be handling more load than other nodes with fewer shards assigned. In this case, random load balancing may exacerbate the load imbalance.

# B3

Trying every node until one succeeds might increase the burden on the system, especially if a given shard has many nodes. This might reduce response times for requests that would otherwise succeed on the first try. For instance, if a node is already overloaded and receives a request as a result of the retry mechanism, and then receives another "new" request from a client, it will prioritize the retry request over the new request. The new request will wait in a queue until the retry request is processed, which will increase the response time for the new request. To address this issue, you could implement a more sophisticated load balancing strategy that takes into account the load on each node and assigns requests to nodes accordingly. 

Or, you could implement a priority queue (with a timeout for retries) to ensure that new requests are prioritized over retry attempts. 

# B4

Partial failures on Set calls can lead to inconsistent state across nodes, leading to future calls to `Get(key)` potentially returning no value when a value for `key` might exist on some node in the shard. Similarly, future calls to `Get(key)` might return a stale value, if the most recent `Set(key)` was not propagated to all nodes in the shard. 

In addition, if a `Set(key)` call fails on all nodes, the client might observe that the value for `key` is not set, even though the client attempted to set it. This could lead to data loss or inconsistency in the system. 

# D2

go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --set-qps 200 --get-qps 600

Stress test completed!
Get requests: 35169/35169 succeeded = 100.000000% success rate
Set requests: 12009/12009 succeeded = 100.000000% success rate
Correct responses: 34889/34889 = 100.000000%
Total requests: 47178 = 786.154895 QPS

The initial test gave me 130 QPS, and this is roughly 6 times that, which makes sense since I just multiplied the get-qps and set-qps by 6.

go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --set-qps 200 --get-qps 600 --ttl 0.5s

Stress test completed!
Get requests: 34627/34627 succeeded = 100.000000% success rate
Set requests: 11984/11984 succeeded = 100.000000% success rate
Correct responses: 34279/34279 = 100.000000%
Total requests: 46611 = 776.695713 QPS

I then tried changing the ttl to 25% of the default of 2s and got only a slight decrease in QPS, which was originally surprising but now makes sense because this just means that my 5 second cleaning goroutine is being more productive, just slightly more productive.