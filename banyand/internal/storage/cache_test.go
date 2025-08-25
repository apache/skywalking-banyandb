// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// testSizableString is a test implementation of Sizable for string values.
type testSizableString struct {
	value string
}

func (s testSizableString) Size() uint64 {
	return uint64(len(s.value)) + 16 // base struct overhead
}

func (s testSizableString) String() string {
	return s.value
}

func TestCachePutAndGet(t *testing.T) {
	serviceCache := NewServiceCache()

	key := EntryKey{
		group:     "test-group",
		partID:    0,
		offset:    0,
		segmentID: segmentID(0),
		shardID:   common.ShardID(0),
	}
	value := testSizableString{value: "test-value"}

	serviceCache.Put(key, value)
	assert.Equal(t, uint64(1), serviceCache.Entries())
	result := serviceCache.Get(key)
	assert.Equal(t, value, result)
	assert.Equal(t, uint64(1), serviceCache.Requests())
	assert.Equal(t, uint64(0), serviceCache.Misses())

	nonExistentKey := EntryKey{
		group:     "test-group",
		partID:    1,
		offset:    100,
		segmentID: segmentID(1),
		shardID:   common.ShardID(1),
	}

	result = serviceCache.Get(nonExistentKey)
	assert.Nil(t, result)
	assert.Equal(t, uint64(2), serviceCache.Requests())
	assert.Equal(t, uint64(1), serviceCache.Misses())
}

func TestCacheEvict(t *testing.T) {
	serviceCache := NewServiceCache().(*serviceCache)

	// Calculate the size of one entry to set appropriate cache size
	testValue := testSizableString{value: "v0"}
	valueSize := testValue.Size()
	// Each entry has overhead: unsafe.Sizeof(entry{}) + unsafe.Sizeof(entryIndex{}) + unsafe.Sizeof(EntryKey{})
	// Setting cache size to allow only 1 entry plus some buffer
	serviceCache.maxCacheSize = valueSize + 150 // Enough for 1 entry with overhead

	var expectedKey EntryKey
	var expectedValue testSizableString
	for i := 0; i < 10; i++ {
		key := EntryKey{
			group:     "test-group",
			partID:    uint64(i),
			offset:    uint64(i * 100),
			segmentID: segmentID(i),
			shardID:   common.ShardID(i),
		}
		value := testSizableString{value: "v" + strconv.Itoa(i)}
		serviceCache.Put(key, value)
		if i == 9 {
			expectedKey, expectedValue = key, value
		}
	}

	assert.Equal(t, uint64(1), serviceCache.Entries())
	result := serviceCache.Get(expectedKey)
	assert.Equal(t, expectedValue, result)
}

func TestCacheClean(t *testing.T) {
	config := CacheConfig{
		MaxCacheSize:    run.Bytes(100 * 1024 * 1024),
		CleanupInterval: 10 * time.Millisecond,
		IdleTimeout:     10 * time.Millisecond,
	}
	serviceCache := NewServiceCacheWithConfig(config).(*serviceCache)
	defer serviceCache.Close()

	key := EntryKey{
		group:     "test-group",
		partID:    0,
		offset:    0,
		segmentID: segmentID(0),
		shardID:   common.ShardID(0),
	}
	value := testSizableString{value: "test-value"}

	serviceCache.Put(key, value)
	assert.Equal(t, uint64(1), serviceCache.Entries())
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, uint64(0), serviceCache.Entries())
	assert.Nil(t, serviceCache.Get(key))
}

func TestCacheClose(t *testing.T) {
	serviceCache := NewServiceCache().(*serviceCache)

	key := EntryKey{
		group:     "test-group",
		partID:    0,
		offset:    0,
		segmentID: segmentID(0),
		shardID:   common.ShardID(0),
	}
	value := testSizableString{value: "test-value"}

	serviceCache.Put(key, value)
	assert.Equal(t, uint64(1), serviceCache.Entries())
	serviceCache.Close()
	assert.Nil(t, serviceCache.entry)
	assert.Nil(t, serviceCache.entryIndex)
	assert.Nil(t, serviceCache.entryIndexHeap)
}

func TestCacheConcurrency(t *testing.T) {
	serviceCache := NewServiceCache()

	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup
	var expectedKey EntryKey
	var expectedValue testSizableString
	wg.Add(numGoroutines * 2)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				num := id*numOperations + j
				key := EntryKey{
					group:     "test-group",
					partID:    uint64(num),
					offset:    uint64(num * 100),
					segmentID: segmentID(num),
					shardID:   common.ShardID(num),
				}
				value := testSizableString{value: "test-value" + strconv.Itoa(num)}
				if id == 0 && j == 0 {
					expectedKey, expectedValue = key, value
				}
				serviceCache.Put(key, value)
			}
		}(i)
	}
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				num := id*numOperations + j
				key := EntryKey{
					group:     "test-group",
					partID:    uint64(num),
					offset:    uint64(num * 100),
					segmentID: segmentID(num),
					shardID:   common.ShardID(num),
				}
				serviceCache.Get(key)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, uint64(numGoroutines*numOperations), serviceCache.Entries())
	assert.Equal(t, uint64(numGoroutines*numOperations), serviceCache.Requests())
	result := serviceCache.Get(expectedKey)
	assert.Equal(t, expectedValue, result)
}
