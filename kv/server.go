package kv

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Define an internal struct to hold each key's value and expiration
type kvEntry struct {
	value  string
	expiry time.Time // Expiry timestamp based on TTL
}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	shardData  map[int]map[string]kvEntry // Data partitioned by shards
	shardLocks map[int]*sync.RWMutex      // Each shard has its own RWMutex; protects shardData

	trackedShards map[int]struct{} // Tracks which shards this server currently owns
}

func (server *KvServerImpl) handleShardMapUpdate() {
	currentShards := make(map[int]struct{})
	for _, shardID := range server.shardMap.ShardsForNode(server.nodeName) {
		currentShards[shardID] = struct{}{}
	}

	// Identify shards to add and remove
	shardsToAdd := map[int]struct{}{}
	shardsToRemove := map[int]struct{}{}

	// Find shards that are in currentShards but not in trackedShards -> these are shards to add
	for shardID := range currentShards {
		if _, ok := server.trackedShards[shardID]; !ok {
			shardsToAdd[shardID] = struct{}{}
		}
	}

	// Find shards that are in trackedShards but not in currentShards -> these are shards to remove
	for shardID := range server.trackedShards {
		if _, ok := currentShards[shardID]; !ok {
			shardsToRemove[shardID] = struct{}{}
		}
	}

	// Process shard additions
	for shardID := range shardsToAdd {
		// Initialize the lock for the shard if it does not exist
		if _, ok := server.shardLocks[shardID]; !ok {
			server.shardLocks[shardID] = &sync.RWMutex{}
		}
		server.shardLocks[shardID].Lock()

		// Clear `shardData` if it exists or initialize it if not
		if _, ok := server.shardData[shardID]; ok {
			clear(server.shardData[shardID])
		} else {
			server.shardData[shardID] = make(map[string]kvEntry)
		}
		server.trackedShards[shardID] = struct{}{} // Mark the shard as tracked

		// use the `GetShardContents` RPC to copy data in any case that a shard is added to a node

		nodeNames := server.shardMap.NodesForShard(shardID)

		// If there are no peers available for a given shard,
		// log an error and initialize the shard as empty.
		if len(nodeNames) == 0 {
			logrus.WithField("node", server.nodeName).Errorf("handleShardMapUpdate(): no peers available for shard %v", shardID)
			server.shardLocks[shardID].Unlock()
			continue
		} else if len(nodeNames) == 1 && nodeNames[0] == server.nodeName {
			logrus.WithField("node", server.nodeName).Errorf("handleShardMapUpdate(): no other peers available for shard %v", shardID)
			server.shardLocks[shardID].Unlock()
			continue
		}

		attemptedNodes := map[string]struct{}{}
		attemptedNodes[server.nodeName] = struct{}{} // Be sure not to try to call GetShardContents on the current node

		var client proto.KvClient
		var resp *proto.GetShardContentsResponse
		var err error
		success := false
		for len(attemptedNodes) < len(nodeNames) {
			// If possible, spread your GetShardContents requests out using some load balancing strategy
			// Random load balancing: pick a random node to send the request to.
			// Given enough requests and a sufficiently good randomization,
			// this spreads the load among the nodes fairly.
			node := nodeNames[rand.Intn(len(nodeNames))]

			// Do not try the same node twice
			if _, ok := attemptedNodes[node]; ok {
				continue
			}
			attemptedNodes[node] = struct{}{}

			// Use the provided ClientPool and GetClient to get a KvClient to a peer node.
			client, err = server.clientPool.GetClient(node)
			// If a `GetClient` fails for a given node, try another.
			if err != nil {
				continue
			}

			resp, err = client.GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shardID)})
			// If a `GetShardContents` request fails, try another.
			if err != nil {
				continue
			}

			for _, value := range resp.GetValues() {
				server.shardData[shardID][value.Key] = kvEntry{
					value:  value.Value,
					expiry: time.Now().Add(time.Duration(value.TtlMsRemaining) * time.Millisecond),
				}
			}
			success = true
			break
		}

		// If all peers fail, log an error and initialize the shard as empty.
		if !success {
			logrus.WithField("node", server.nodeName).Errorf("handleShardMapUpdate(): no nodes returned a successful response for shard %v; tried %v (error: %v)", shardID, attemptedNodes, err)
		}

		server.shardLocks[shardID].Unlock()
	}

	// Process shard removals
	for shardID := range shardsToRemove {
		// Lock the shard to ensure no one else is accessing it during removal
		lock := server.shardLocks[shardID]
		lock.Lock()

		// Clear the shard data
		delete(server.shardData, shardID)

		// Mark the shard as untracked
		delete(server.trackedShards, shardID)

		// Remove the lock
		delete(server.shardLocks, shardID)

		// Unlock the shard
		lock.Unlock()
	}

}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:      nodeName,
		shardMap:      shardMap,
		listener:      &listener,
		clientPool:    clientPool,
		shutdown:      make(chan struct{}),
		shardData:     make(map[int]map[string]kvEntry),
		shardLocks:    make(map[int]*sync.RWMutex),
		trackedShards: make(map[int]struct{}),
	}

	go server.cleanupExpiredEntries()
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	close(server.shutdown)
	server.listener.Close()

	clear(server.shardData)
	clear(server.shardLocks)
	clear(server.trackedShards)
}

func (server *KvServerImpl) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	// Validate the key
	if request.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "Key cannot be empty")
	}

	// Get the nodes responsible for the key's shard
	nodes := GetNodesForKey(request.GetKey(), server.shardMap)

	// Check if this server hosts the shard
	if !contains(nodes, server.nodeName) {
		return nil, status.Error(codes.NotFound, "Server does not host shard for this key")
	}

	// Determine the shard for this key
	shardID := GetShardForKey(request.GetKey(), server.shardMap.NumShards())

	// Lock the specific shard for reading
	server.shardLocks[shardID].RLock()
	defer server.shardLocks[shardID].RUnlock()

	// Look up the entry in the correct shard
	entry, found := server.shardData[shardID][request.GetKey()]
	if !found || time.Now().After(entry.expiry) {
		// Either key does not exist or is expired
		return &proto.GetResponse{
			Value:    "",
			WasFound: false,
		}, nil
	}

	// Return the value and WasFound: true if the entry exists and is not expired
	return &proto.GetResponse{
		Value:    entry.value,
		WasFound: true,
	}, nil
}

func (server *KvServerImpl) Set(ctx context.Context, request *proto.SetRequest) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	// Validate the key: gRPC spec requires keys to be non-empty.
	if request.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "Key cannot be empty")
	}

	// Get the nodes responsible for the key's shard
	nodes := GetNodesForKey(request.GetKey(), server.shardMap)

	if !contains(nodes, server.nodeName) {
		return nil, status.Error(codes.NotFound, "Server does not host shard for this key")
	}

	// Calculate the expiry time based on TTL (in milliseconds).
	expiry := time.Now().Add(time.Duration(request.GetTtlMs()) * time.Millisecond)

	// Determine the shard for this key
	shardID := GetShardForKey(request.GetKey(), server.shardMap.NumShards())

	// Lock the specific shard for writing
	server.shardLocks[shardID].Lock()
	defer server.shardLocks[shardID].Unlock()

	// Set the key-value pair in the correct shard
	server.shardData[shardID][request.GetKey()] = kvEntry{
		value:  request.Value,
		expiry: expiry,
	}

	// Return an empty SetResponse on success.
	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	// Validate the key
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key cannot be empty")
	}

	// Get the nodes responsible for the key's shard
	nodes := GetNodesForKey(request.GetKey(), server.shardMap)

	// Check if this server hosts the shard
	if !contains(nodes, server.nodeName) {
		return nil, status.Error(codes.NotFound, "Server does not host shard for this key")
	}

	// Determine the shard for this key
	shardID := GetShardForKey(request.GetKey(), server.shardMap.NumShards())

	// Lock the specific shard for writing
	server.shardLocks[shardID].Lock()
	defer server.shardLocks[shardID].Unlock()

	// Delete the entry from the correct shard
	delete(server.shardData[shardID], request.GetKey())

	// Return an empty DeleteResponse on success
	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "shard": request.Shard},
	).Trace("node received GetShardContents() request")

	// `GetShardContents` should fail if the server does not host the shard
	shard := int(request.GetShard())
	nodeNames := server.shardMap.NodesForShard(shard)
	if nodeNames == nil || !contains(nodeNames, server.nodeName) {
		return nil, status.Errorf(codes.NotFound, "Server %v does not host shard %v", server.nodeName, shard)
	}

	// GetShardContents should return the subset of keys along with their
	// values and remaining time on their expiry for a given shard.
	contents := make([]*proto.GetShardValue, 0)
	server.shardLocks[shard].RLock()
	defer server.shardLocks[shard].RUnlock()
	for key, entry := range server.shardData[shard] {
		// All values for only the requested shard should be sent via `GetShardContents`
		if GetShardForKey(key, server.shardMap.NumShards()) == shard {
			// `TtlMsRemaining` should be the time remaining between now and
			// the expiry time, not the original value of the TTL. This
			// ensures that the key does not live significantly longer when
			// it is copied to another node.
			contents = append(contents, &proto.GetShardValue{
				Key:            key,
				Value:          entry.value,
				TtlMsRemaining: time.Until(entry.expiry).Milliseconds(),
			})
		}
	}
	return &proto.GetShardContentsResponse{Values: contents}, nil
}

func (server *KvServerImpl) cleanupExpiredEntries() {
	ticker := time.NewTicker(5 * time.Second) // Run cleanup every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-server.shutdown:
			return // Exit when shutdown is triggered
		case <-ticker.C:
			now := time.Now()
			for shardID := range server.shardLocks {
				server.shardLocks[shardID].Lock()
				for key, entry := range server.shardData[shardID] {
					if now.After(entry.expiry) {
						delete(server.shardData[shardID], key)
					}
				}
				server.shardLocks[shardID].Unlock()
			}
		}
	}
}

func contains(nodes []string, nodeName string) bool {
	for _, node := range nodes {
		if node == nodeName {
			return true
		}
	}
	return false
}
