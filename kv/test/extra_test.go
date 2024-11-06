package kvtest

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

// TestSetAndGet verifies that Set operations store data correctly
// and that Get operations retrieve the correct values.
func TestSetAndGet(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	defer setup.Shutdown()

	// Basic Set and Get
	err := setup.NodeSet("n1", "key1", "value1", 5*time.Second)
	assert.Nil(t, err)

	val, wasFound, err := setup.NodeGet("n1", "key1")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "value1", val)

	// Test retrieving a non-existent key
	_, wasFound, err = setup.NodeGet("n1", "nonexistent")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

// TestSetWithTTL verifies that data expires correctly based on TTL.
func TestSetWithTTL(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	defer setup.Shutdown()

	// Set a key with a short TTL
	err := setup.NodeSet("n1", "temp-key", "temp-value", 500*time.Millisecond)
	assert.Nil(t, err)

	// Immediately verify the key is present
	val, wasFound, err := setup.NodeGet("n1", "temp-key")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "temp-value", val)

	// Wait for the TTL to expire
	time.Sleep(600 * time.Millisecond)

	// Verify the key has expired and is no longer accessible
	_, wasFound, err = setup.NodeGet("n1", "temp-key")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

// TestDeleteNonExistentKey ensures Delete operations handle non-existent keys gracefully.
func TestDeleteNonExistentKey(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	defer setup.Shutdown()

	// Attempt to delete a non-existent key
	err := setup.NodeDelete("n1", "nonexistent-key")
	assert.Nil(t, err) // Deleting a non-existent key should not result in an error
}

// TestConcurrentSetsSingleKey tests concurrent SET operations on a single key in one node.
func TestConcurrentSetsSingleKey(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	defer setup.Shutdown()
	var wg sync.WaitGroup
	goros := runtime.NumCPU() * 2
	iters := 100

	for i := 0; i < goros; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < iters; j++ {
				err := setup.NodeSet("n1", "abc", fmt.Sprintf("value-%d", j), 5*time.Second)
				assert.Nil(t, err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// Verify final value on the key
	val, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Contains(t, val, "value-")
}

// TestConcurrentGetsMultipleKeys tests concurrent GET operations on multiple keys in one node.
func TestConcurrentGetsMultipleKeys(t *testing.T) {
	setup := MakeTestSetup(MakeMultiShardSingleNode())
	defer setup.Shutdown()
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	// Set each key-value pair
	for i, key := range keys {
		err := setup.NodeSet("n1", key, values[i], 5*time.Second)
		assert.Nil(t, err)
	}

	var wg sync.WaitGroup
	goros := runtime.NumCPU() * 2
	iters := 100

	for i := 0; i < goros; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < iters; j++ {
				for i, key := range keys {
					val, wasFound, err := setup.NodeGet("n1", key)
					assert.Nil(t, err)
					assert.True(t, wasFound)
					assert.Equal(t, values[i], val)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
