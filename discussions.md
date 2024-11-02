# B2

Random load balancing assumes that all nodes within a shard are always homogeneous; that is, it assumes all nodes have equal capability to handle requests, which is not necessarily true. For example, some nodes with higher CPU resources will likely be able to handle more requests, so a uniform load balacing is not ideal, because this underutilizes CPU. 

Similarly, if a node has more shards assigned to it, it may already be handling more load than other nodes with fewer shards assigned. In this case, random load balancing may exacerbate the load imbalance.
