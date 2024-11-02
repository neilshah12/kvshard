package kv

import (
	"context"
	"math/rand"
	"sync"
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

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	// find the nodes which host the key in the request
	nodeNames := GetNodesForKey(key, kv.shardMap)

	// If no nodes are available (none host the shard), return an error.
	if len(nodeNames) == 0 {
		return "", false, status.Errorf(codes.NotFound, "no nodes host the shard for key %s", key)
	}

	// failover `Get()` calls that fail to another node
	attemptedNodes := make(map[string]struct{})
	var client pb.KvClient
	var resp *pb.GetResponse
	var err error
	for len(attemptedNodes) < len(nodeNames) {
		// Random load balancing: pick a random node to send the request to.
		// Given enough requests and a sufficiently good randomization,
		// this spreads the load among the nodes fairly.
		node := nodeNames[rand.Intn(len(nodeNames))]

		// Do not try the same node twice for the same call to Get.
		if _, ok := attemptedNodes[node]; ok {
			continue
		}
		attemptedNodes[node] = struct{}{}

		// If `ClientPool.GetClient()` fails or `KvClient.Get()` fails for a single node, try another node.
		client, err = kv.clientPool.GetClient(node)
		if err != nil {
			continue
		}
		resp, err = client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			continue
		}

		// Return the first successful response from any node in the set.
		return resp.GetValue(), resp.GetWasFound(), nil
	}

	// If no node returns a successful response, return the last error heard from any node.
	return resp.GetValue(), resp.GetWasFound(), err
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	// find the nodes which host the key in the request
	nodeNames := GetNodesForKey(key, kv.shardMap)

	// If no nodes are available (none host the shard), return an error.
	if len(nodeNames) == 0 {
		return status.Errorf(codes.NotFound, "no nodes host the shard for key %s", key)
	}

	// fan out to all nodes which host the shard
	var wg sync.WaitGroup
	var err error
	for _, nodeName := range nodeNames {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			client, getClientErr := kv.clientPool.GetClient(node)
			if getClientErr != nil {
				err = getClientErr
				return
			}
			_, setErr := client.Set(ctx, &pb.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})
			if setErr != nil {
				err = setErr
				return
			}
		}(nodeName)
	}
	wg.Wait()
	return err
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	// find the nodes which host the key in the request
	nodeNames := GetNodesForKey(key, kv.shardMap)

	// If no nodes are available (none host the shard), return an error.
	if len(nodeNames) == 0 {
		return status.Errorf(codes.NotFound, "no nodes host the shard for key %s", key)
	}

	// fan out to all nodes which host the shard
	var wg sync.WaitGroup
	var err error
	for _, nodeName := range nodeNames {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()

			client, getClientErr := kv.clientPool.GetClient(node)
			if getClientErr != nil {
				err = getClientErr
				return
			}
			_, deleteErr := client.Delete(ctx, &pb.DeleteRequest{Key: key})
			if deleteErr != nil {
				err = deleteErr
				return
			}
		}(nodeName)
	}
	wg.Wait()
	return err
}
