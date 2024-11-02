package kv

import (
	"context"
	"time"

	pb "cs426.yale.edu/lab4/kv/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func getNodesForKey(key string, shardMap *ShardMap) []string {
	// calculate the appropriate shard
	shard := GetShardForKey(key, shardMap.NumShards())

	// Use the provided `ShardMap` instance in `Kv.shardMap` to find the set of nodes which host the shard.
	nodeNames := shardMap.NodesForShard(shard)

	return nodeNames
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	// find the nodes which host the key in the request
	nodeNames := getNodesForKey(key, kv.shardMap)

	// If no nodes are available (none host the shard), return an error.
	if len(nodeNames) == 0 {
		return "", false, status.Errorf(codes.NotFound, "no nodes host the shard for key %s", key)
	}

	// pick any node name which hosts the shard and
	// use the provided `ClientPool.GetClient` to get a `KvClient` to use to send the request.
	client, err := kv.clientPool.GetClient(nodeNames[0])
	if err != nil {
		return "", false, err
	}

	// create a `GetRequest` and send it with `KvClient.Get`
	resp, err := client.Get(context.TODO(), &pb.GetRequest{Key: key})
	if err != nil {
		return resp.GetValue(), resp.GetWasFound(), err
	}

	// If a request to a node is successful, return `(Value, WasFound, nil)`
	return resp.GetValue(), resp.GetWasFound(), nil
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	panic("TODO: Part B")
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	panic("TODO: Part B")
}
