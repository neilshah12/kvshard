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

const numShards = 16 // Define a reasonable number of shards (16 is a typical choice)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	shards []map[string]kvEntry // Data is partitioned across multiple maps
	locks  []sync.RWMutex       // Each shard has its own RWMutex
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
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		shards:     make([]map[string]kvEntry, numShards),
		locks:      make([]sync.RWMutex, numShards),
	}

	// Initialize each shard as an empty map
	for i := 0; i < numShards; i++ {
		server.shards[i] = make(map[string]kvEntry)
	}

	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
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

	// Determine the shard for this key
	shardIndex := GetShardForKey(request.GetKey(), numShards) - 1

	// Lock the specific shard for reading
	server.locks[shardIndex].RLock()
	defer server.locks[shardIndex].RUnlock()

	// Look up the entry in the correct shard
	entry, found := server.shards[shardIndex][request.GetKey()]
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

	// Calculate the expiry time based on TTL (in milliseconds).
	expiry := time.Now().Add(time.Duration(request.GetTtlMs()) * time.Millisecond)

	// Determine the shard for this key
	shardIndex := GetShardForKey(request.GetKey(), numShards) - 1

	// Lock the specific shard for writing
	server.locks[shardIndex].Lock()
	defer server.locks[shardIndex].Unlock()

	// Set the key-value pair in the correct shard
	server.shards[shardIndex][request.GetKey()] = kvEntry{
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

	shardIndex := GetShardForKey(request.GetKey(), numShards) - 1

	// Lock the specific shard for writing
	server.locks[shardIndex].Lock()
	defer server.locks[shardIndex].Unlock()

	// Delete the entry from the correct shard
	delete(server.shards[shardIndex], request.GetKey())

	// Return an empty DeleteResponse on success
	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	panic("TODO: Part C")
}
