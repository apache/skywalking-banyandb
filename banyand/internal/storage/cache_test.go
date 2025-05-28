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
)

func TestCachePutAndGet(t *testing.T) {
	cache := NewCache()

	key := EntryKey{
		group:     "test-group",
		PartID:    0,
		Offset:    0,
		segmentID: segmentID(0),
		shardID:   common.ShardID(0),
	}
	value := "test-value"

	cache.Put(key, value)
	assert.Equal(t, uint64(1), cache.Len())
	result := cache.Get(key)
	assert.Equal(t, value, result)
	assert.Equal(t, uint64(1), cache.Requests())
	assert.Equal(t, uint64(0), cache.Misses())

	nonExistentKey := EntryKey{
		group:     "test-group",
		PartID:    1,
		Offset:    100,
		segmentID: segmentID(1),
		shardID:   common.ShardID(1),
	}

	result = cache.Get(nonExistentKey)
	assert.Nil(t, result)
	assert.Equal(t, uint64(2), cache.Requests())
	assert.Equal(t, uint64(1), cache.Misses())
}

func TestCacheEvict(t *testing.T) {
	cache := NewCache()
	cache.maxCacheSize = 50

	var expectedKey EntryKey
	var expectedValue string
	for i := 0; i < 10; i++ {
		key := EntryKey{
			group:     "test-group",
			PartID:    uint64(i),
			Offset:    uint64(i * 100),
			segmentID: segmentID(i),
			shardID:   common.ShardID(i),
		}
		value := "test-value" + strconv.Itoa(i)
		cache.Put(key, value)
		if i == 9 {
			expectedKey, expectedValue = key, value
		}
	}

	assert.Equal(t, uint64(1), cache.Len())
	result := cache.Get(expectedKey)
	assert.Equal(t, expectedValue, result)
}

func TestCacheClean(t *testing.T) {
	cache := NewCache()
	cache.idleTimeout = 10 * time.Millisecond
	cache.cleanupInterval = 10 * time.Millisecond
	go cache.Clean()
	defer cache.Close()

	key := EntryKey{
		group:     "test-group",
		PartID:    0,
		Offset:    0,
		segmentID: segmentID(0),
		shardID:   common.ShardID(0),
	}
	value := "test-value"

	cache.Put(key, value)
	assert.Equal(t, uint64(1), cache.Len())
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, uint64(0), cache.Len())
	assert.Nil(t, cache.Get(key))
}

func TestCacheClose(t *testing.T) {
	cache := NewCache()
	go cache.Clean()

	key := EntryKey{
		group:     "test-group",
		PartID:    0,
		Offset:    0,
		segmentID: segmentID(0),
		shardID:   common.ShardID(0),
	}
	value := "test-value"

	cache.Put(key, value)
	assert.Equal(t, uint64(1), cache.Len())
	cache.Close()
	assert.Nil(t, cache.entry)
	assert.Nil(t, cache.entryIndex)
	assert.Nil(t, cache.entryIndexHeap)
}

func TestCacheConcurrency(t *testing.T) {
	cache := NewCache()

	const numGoroutines = 10
	const numOperations = 100
	var wg sync.WaitGroup
	var expectedKey EntryKey
	var expectedValue string
	wg.Add(numGoroutines * 2)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				num := id*numOperations + j
				key := EntryKey{
					group:     "test-group",
					PartID:    uint64(num),
					Offset:    uint64(num * 100),
					segmentID: segmentID(num),
					shardID:   common.ShardID(num),
				}
				value := "test-value" + strconv.Itoa(num)
				if id == 0 && j == 0 {
					expectedKey, expectedValue = key, value
				}
				cache.Put(key, value)
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
					PartID:    uint64(num),
					Offset:    uint64(num * 100),
					segmentID: segmentID(num),
					shardID:   common.ShardID(num),
				}
				cache.Get(key)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, uint64(numGoroutines*numOperations), cache.Len())
	assert.Equal(t, uint64(numGoroutines*numOperations), cache.Requests())
	result := cache.Get(expectedKey)
	assert.Equal(t, expectedValue, result)
}
