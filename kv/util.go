package kv

import "hash/fnv"

/// This file can be used for any common code you want to define and separate
/// out from server.go or client.go

func GetNodesForKey(key string, shardMap *ShardMap) []string {
	// calculate the appropriate shard
	shard := GetShardForKey(key, shardMap.NumShards())

	// Use the provided `ShardMap` instance in `Kv.shardMap` to find the set of nodes which host the shard
	nodeNames := shardMap.NodesForShard(shard)

	return nodeNames
}

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}
