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

	shardData      map[int]map[string]kvEntry // Data partitioned by shards
	shardLocks     map[int]*sync.RWMutex      // Each shard has its own RWMutex; shardLocks[shard] protects shardData[shard]
	shardLocksLock sync.RWMutex               // protects shardData and shardLocks
}

func (server *KvServerImpl) handleShardMapUpdate() {
	currentShards := map[int]struct{}{}
	for _, shardID := range server.shardMap.ShardsForNode(server.nodeName) {
		currentShards[shardID] = struct{}{}
	}

	server.shardLocksLock.RLock()

	// Find shards that are in currentShards but not in shardData -> these are shards to add
	shardsToAdd := map[int]struct{}{}
	for shardID := range currentShards {
		if _, ok := server.shardData[shardID]; !ok {
			shardsToAdd[shardID] = struct{}{}
		}
	}

	// Find shards that are in shardData but not in currentShards -> these are shards to remove
	shardsToRemove := map[int]struct{}{}
	for shardID := range server.shardData {
		if _, ok := currentShards[shardID]; !ok {
			shardsToRemove[shardID] = struct{}{}
		}
	}

	server.shardLocksLock.RUnlock()

	// Process shard additions
	for shardID := range shardsToAdd {
		server.shardLocksLock.RLock()
		_, isShardTracked := server.shardLocks[shardID]
		server.shardLocksLock.RUnlock()

		// If the shard is not already tracked, then create entries in `shardLocks` and `shardData`
		server.shardLocksLock.Lock()
		if isShardTracked {
			server.shardLocks[shardID].Lock()
			clear(server.shardData[shardID])
			server.shardLocks[shardID].Unlock()
		} else {
			server.shardLocks[shardID] = &sync.RWMutex{}
			server.shardData[shardID] = make(map[string]kvEntry)
		}
		server.shardLocksLock.Unlock()

		// use the `GetShardContents` RPC to copy data in any case that a shard is added to a node

		nodeNames := server.shardMap.NodesForShard(shardID)

		// If there are no peers available for a given shard,
		// log an error and initialize the shard as empty.
		if len(nodeNames) == 0 {
			logrus.WithField("node", server.nodeName).Errorf("handleShardMapUpdate(): no peers available for shard %v", shardID)
			continue
		} else if len(nodeNames) == 1 && nodeNames[0] == server.nodeName {
			logrus.WithField("node", server.nodeName).Errorf("handleShardMapUpdate(): no other peers available for shard %v", shardID)
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

			server.shardLocks[shardID].Lock()
			for _, value := range resp.GetValues() {
				server.shardData[shardID][value.Key] = kvEntry{
					value:  value.Value,
					expiry: time.Now().Add(time.Duration(value.TtlMsRemaining) * time.Millisecond),
				}
			}
			server.shardLocks[shardID].Unlock()
			success = true
			break
		}

		// If all peers fail, log an error and initialize the shard as empty.
		if !success {
			logrus.WithField("node", server.nodeName).Errorf("handleShardMapUpdate(): no nodes returned a successful response for shard %v; tried %v (error: %v)", shardID, attemptedNodes, err)
		}
	}

	// Process shard removals
	for shardID := range shardsToRemove {
		server.shardLocksLock.Lock()

		// Lock the shard to ensure no one else is accessing it during removal
		lock := server.shardLocks[shardID]
		lock.Lock()

		// Clear the shard data
		delete(server.shardData, shardID)

		// Remove the lock
		delete(server.shardLocks, shardID)

		// Unlock the shard
		lock.Unlock()

		server.shardLocksLock.Unlock()
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
		nodeName:       nodeName,
		shardMap:       shardMap,
		listener:       &listener,
		clientPool:     clientPool,
		shutdown:       make(chan struct{}),
		shardData:      make(map[int]map[string]kvEntry),
		shardLocks:     make(map[int]*sync.RWMutex),
		shardLocksLock: sync.RWMutex{},
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

	// Check if this server hosts the shard
	shardID := GetShardForKey(request.GetKey(), server.shardMap.NumShards())
	server.shardLocksLock.RLock()
	defer server.shardLocksLock.RUnlock()
	if _, ok := server.shardLocks[shardID]; !ok {
		return nil, status.Errorf(codes.NotFound, "Server %v does not host shard %v", server.nodeName, shardID)
	}
	lock := server.shardLocks[shardID]
	lock.RLock()
	defer lock.RUnlock()

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

	// Check if this server hosts the shard
	shardID := GetShardForKey(request.GetKey(), server.shardMap.NumShards())
	server.shardLocksLock.RLock()
	defer server.shardLocksLock.RUnlock()
	if _, ok := server.shardLocks[shardID]; !ok {
		return nil, status.Errorf(codes.NotFound, "Server %v does not host shard %v", server.nodeName, shardID)
	}
	lock := server.shardLocks[shardID]
	lock.Lock()
	defer lock.Unlock()

	// Calculate the expiry time based on TTL (in milliseconds).
	expiry := time.Now().Add(time.Duration(request.GetTtlMs()) * time.Millisecond)

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

	// Check if this server hosts the shard
	shardID := GetShardForKey(request.GetKey(), server.shardMap.NumShards())
	server.shardLocksLock.RLock()
	defer server.shardLocksLock.RUnlock()
	if _, ok := server.shardLocks[shardID]; !ok {
		return nil, status.Errorf(codes.NotFound, "Server %v does not host shard %v", server.nodeName, shardID)
	}
	lock := server.shardLocks[shardID]
	lock.Lock()
	defer lock.Unlock()

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

	shard := int(request.GetShard())

	// `GetShardContents` should fail if the server does not host the shard
	server.shardLocksLock.RLock()
	defer server.shardLocksLock.RUnlock()
	if _, ok := server.shardLocks[shard]; !ok {
		return nil, status.Errorf(codes.NotFound, "Server %v does not host shard %v", server.nodeName, shard)
	}
	lock := server.shardLocks[shard]
	lock.RLock()
	defer lock.RUnlock()

	// GetShardContents should return the subset of keys along with their
	// values and remaining time on their expiry for a given shard.
	contents := []*proto.GetShardValue{}
	for key, entry := range server.shardData[shard] {
		// All values for only the requested shard should be sent via `GetShardContents`.
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
			server.shardLocksLock.RLock()
			for shardID := range server.shardLocks {
				server.shardLocks[shardID].Lock()
				for key, entry := range server.shardData[shardID] {
					if now.After(entry.expiry) {
						delete(server.shardData[shardID], key)
					}
				}
				server.shardLocks[shardID].Unlock()
			}
			server.shardLocksLock.RUnlock()
		}
	}
}
