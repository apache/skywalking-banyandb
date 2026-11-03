// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"errors"
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const (
	groupKeyChunkBytes             = 64 << 10
	groupKeyChunkHeaderBytes int64 = 24
)

type groupKey struct {
	chunk  uint32
	offset uint32
	length uint32
}

type groupKeyChunk struct {
	data []byte
}

// Only fixed-size chunks enter the pool. Its aggregate idle capacity is bounded
// across all queries; smaller and oversized allocations remain query-owned.
var groupKeyChunkPool = pool.RegisterBounded("query_group_key_chunks", 8<<20,
	func() *groupKeyChunk { return &groupKeyChunk{data: make([]byte, 0, groupKeyChunkBytes)} },
	func(chunk *groupKeyChunk) int64 { return int64(cap(chunk.data)) + groupKeyChunkHeaderBytes })

func (g *aggregateGroups) storeKey(key []byte) (groupKey, error) {
	if len(key) == 0 {
		return groupKey{}, nil
	}
	if uint64(len(key)) > math.MaxUint32 || uint64(len(g.chunks)) > math.MaxUint32 {
		return groupKey{}, fmt.Errorf("%w: aggregate key exceeds supported arena size", vectorized.ErrMemoryBudgetExceeded)
	}
	var chunk *groupKeyChunk
	if len(g.chunks) > 0 {
		chunk = g.chunks[len(g.chunks)-1]
	}
	if chunk == nil || len(key) > cap(chunk.data)-len(chunk.data) {
		var err error
		chunk, err = g.addKeyChunk(len(key))
		if err != nil {
			return groupKey{}, err
		}
	}
	ref := groupKey{chunk: uint32(len(g.chunks) - 1), offset: uint32(len(chunk.data)), length: uint32(len(key))}
	chunk.data = append(chunk.data, key...)
	return ref, nil
}

func (g *aggregateGroups) addKeyChunk(keyBytes int) (*groupKeyChunk, error) {
	capacity := 256
	if len(g.chunks) > 0 {
		capacity = min(groupKeyChunkBytes, cap(g.chunks[len(g.chunks)-1].data)*2)
	}
	allocation, err := groupAllocationBytes(max(int64(capacity), int64(keyBytes)))
	if err != nil {
		return nil, err
	}
	pointerCapacity := cap(g.chunks)
	var pointerBytes int64
	if len(g.chunks) == pointerCapacity {
		pointerCapacity = max(1, pointerCapacity*2)
		pointerBytes = int64(pointerCapacity) * 8
	}
	bytes := allocation + groupKeyChunkHeaderBytes + pointerBytes
	if err = g.tracker.Reserve(bytes); err != nil {
		// A small query may have room for the key but not the preferred chunk.
		// Fall back before any allocation or checkout; never charge a fixed minimum.
		if !errors.Is(err, vectorized.ErrMemoryBudgetExceeded) {
			return nil, err
		}
		allocation, err = groupAllocationBytes(int64(keyBytes))
		if err != nil {
			return nil, err
		}
		bytes = allocation + groupKeyChunkHeaderBytes + pointerBytes
		if err = g.tracker.Reserve(bytes); err != nil {
			return nil, err
		}
	}
	g.metrics.reserve(bytes)
	g.keyReserved += bytes
	var chunk *groupKeyChunk
	if allocation == groupKeyChunkBytes {
		chunk = groupKeyChunkPool.Get()
	} else {
		chunk = &groupKeyChunk{data: make([]byte, 0, int(allocation))}
	}
	if pointerBytes != 0 {
		chunks := make([]*groupKeyChunk, len(g.chunks), pointerCapacity)
		copy(chunks, g.chunks)
		oldBytes := int64(cap(g.chunks)) * 8
		g.tracker.Release(oldBytes)
		g.metrics.release(oldBytes)
		g.keyReserved -= oldBytes
		g.chunks = chunks
	}
	g.chunks = append(g.chunks, chunk)
	return chunk, nil
}

func (g *aggregateGroups) key(id int) string {
	ref := g.keys[id]
	if ref.length == 0 {
		return ""
	}
	data := g.chunks[ref.chunk].data[int(ref.offset) : int(ref.offset)+int(ref.length)]
	// Key bytes are appended once and never changed while the operator owns the
	// chunks. The view is read-only and borrowed through the next pull or Close,
	// like the output batch. Egress clones strings before Close can pool chunks.
	return convert.BytesToString(data)
}
