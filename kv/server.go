package kv

import (
	"context"
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

const numStripes = 16 // Define a reasonable number of stripes (16 is a typical choice)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	shardData  map[int]map[string]kvEntry // Data partitioned by shards
	shardLocks map[int]*sync.RWMutex      // Each shard has its own RWMutex

	trackedShards map[int]bool // Tracks which shards this server currently owns
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
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
		trackedShards: make(map[int]bool),
	}

	// Initialize each shard as an empty map and its corresponding lock
	for shardID := range shardMap.ShardsForNode(nodeName) {
		server.shardData[shardID] = make(map[string]kvEntry)
		server.shardLocks[shardID] = &sync.RWMutex{}
		server.trackedShards[shardID] = true
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

	// Get the nodes responsible for the key's shard
	nodes := GetNodesForKey(request.GetKey(), server.shardMap)

	// Check if this server hosts the shard
	if !contains(nodes, server.nodeName) {
		return nil, status.Error(codes.NotFound, "Server does not host shard for this key")
	}

	// Determine the shard for this key
	shardID := GetShardForKey(request.GetKey(), len(server.shardData)) - 1

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
	shardID := GetShardForKey(request.GetKey(), len(server.shardData)) - 1

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

	shardID := GetShardForKey(request.GetKey(), len(server.shardData)) - 1

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
	panic("TODO: Part C")
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
			for shardID := 0; shardID < len(server.shardData); shardID++ {
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
