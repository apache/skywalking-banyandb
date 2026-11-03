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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type aggregateGroups struct {
	metrics      *groupMetrics
	tracker      *vectorized.MemoryTracker
	keys         []groupKey
	chunks       []*groupKeyChunk
	scratch      []byte
	index        []uint32
	capacity     int
	tableBytes   int64
	keyBytes     int64
	keyReserved  int64
	scratchBytes int64
}

// appendGroupValue is the common QL grouping codec. Each cell includes its logical
// type and null marker; variable-width payloads include their length. A null has
// no payload, signed zeros share +0, and all NaN representations share one key.
func appendGroupValue(dst []byte, value aggregateValue) []byte {
	marker := byte(value.typ)
	if value.timestamp {
		marker |= 0x80
	}
	dst = append(dst, marker)
	if value.null {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	switch value.typ {
	case vectorized.ColumnTypeInt64:
		return binary.LittleEndian.AppendUint64(dst, uint64(value.i64))
	case vectorized.ColumnTypeFloat64:
		return binary.LittleEndian.AppendUint64(dst, canonicalGroupFloat(value.f64))
	case vectorized.ColumnTypeString:
		dst = binary.LittleEndian.AppendUint64(dst, uint64(len(value.text)))
		return append(dst, value.text...)
	case vectorized.ColumnTypeBytes:
		dst = binary.LittleEndian.AppendUint64(dst, uint64(len(value.raw)))
		return append(dst, value.raw...)
	default:
		panic("unsupported QL group key type")
	}
}

func canonicalGroupFloat(value float64) uint64 {
	if value == 0 {
		return 0
	}
	if math.IsNaN(value) {
		return 0x7ff8000000000000
	}
	return math.Float64bits(value)
}

func groupValueBytes(value aggregateValue) int64 {
	if value.null {
		return 2
	}
	switch value.typ {
	case vectorized.ColumnTypeString:
		return 10 + int64(len(value.text))
	case vectorized.ColumnTypeBytes:
		return 10 + int64(len(value.raw))
	default:
		return 10
	}
}

func (g *aggregateGroups) encode(values []aggregateValue) error {
	var bytes int64
	for _, value := range values {
		size := groupValueBytes(value)
		if size > math.MaxInt64-bytes {
			return fmt.Errorf("%w: aggregate key size overflows", vectorized.ErrMemoryBudgetExceeded)
		}
		bytes += size
	}
	if bytes > int64(cap(g.scratch)) {
		capacity, capacityErr := groupAllocationBytes(max(int64(64), bytes))
		if capacityErr != nil {
			return capacityErr
		}
		if err := g.tracker.Reserve(capacity); err != nil {
			return err
		}
		g.metrics.reserve(capacity)
		g.scratch = make([]byte, 0, int(capacity))
		g.tracker.Release(g.scratchBytes)
		g.metrics.release(g.scratchBytes)
		g.scratchBytes = capacity
	}
	g.scratch = g.scratch[:0]
	for _, value := range values {
		g.scratch = appendGroupValue(g.scratch, value)
	}
	return nil
}

func (o *aggregateOperator) groupID(values []aggregateValue) (int, error) {
	g := &o.groups
	if err := g.encode(values); err != nil {
		return 0, err
	}
	hash := xxhash.Sum64(g.scratch)
	id, slot, found := g.lookup(g.scratch, hash)
	if found {
		return id, nil
	}
	if len(g.keys) == g.capacity {
		if err := o.growGroups(); err != nil {
			return 0, err
		}
		_, slot, _ = g.lookup(g.scratch, hash)
	}
	key, keyErr := g.storeKey(g.scratch)
	if keyErr != nil {
		return 0, keyErr
	}
	id = len(g.keys)
	g.keys = append(g.keys, key)
	g.index[slot] = uint32(id) + 1
	g.keyBytes += int64(len(g.scratch))
	return id, nil
}

// insert retains a complete key only when it is new. Count-distinct prefixes
// the scalar with its group ID, so identical values in different groups remain independent.
func (g *aggregateGroups) insert(values []aggregateValue) (bool, error) {
	if err := g.encode(values); err != nil {
		return false, err
	}
	hash := xxhash.Sum64(g.scratch)
	_, slot, found := g.lookup(g.scratch, hash)
	if found {
		return false, nil
	}
	if len(g.keys) == g.capacity {
		if err := g.grow(nil); err != nil {
			return false, err
		}
		_, slot, _ = g.lookup(g.scratch, hash)
	}
	key, err := g.storeKey(g.scratch)
	if err != nil {
		return false, err
	}
	g.keys = append(g.keys, key)
	g.index[slot] = uint32(len(g.keys))
	g.keyBytes += int64(len(g.scratch))
	return true, nil
}

func (g *aggregateGroups) lookup(key []byte, hash uint64) (id, slot int, found bool) {
	if len(g.index) == 0 {
		return 0, 0, false
	}
	mask := uint64(len(g.index) - 1)
	position := hash & mask
	for {
		entry := g.index[position]
		if entry == 0 {
			return 0, int(position), false
		}
		id = int(entry - 1)
		if g.key(id) == string(key) {
			return id, int(position), true
		}
		position = (position + 1) & mask
	}
}

func groupAllocationBytes(bytes int64) (int64, error) {
	if bytes == 0 {
		return 0, nil
	}
	// Rounding to a power of two bounds Go's small-object size classes and
	// large-object page rounding without depending on runtime internals.
	capacity := int64(8)
	for capacity < bytes {
		if capacity > math.MaxInt64/2 {
			return 0, fmt.Errorf("%w: aggregate key allocation size overflows", vectorized.ErrMemoryBudgetExceeded)
		}
		capacity *= 2
	}
	return capacity, nil
}

func (o *aggregateOperator) growGroups() error {
	return o.groups.grow(o.states)
}

func (g *aggregateGroups) grow(states []aggregateState) error {
	if g.capacity > math.MaxUint32/2 || g.capacity > math.MaxInt/4 {
		return fmt.Errorf("%w: aggregate group count exceeds supported index size", vectorized.ErrMemoryBudgetExceeded)
	}
	capacity := max(1, g.capacity*2)
	var lanes int64
	for index := range states {
		lanes += int64(states[index].lanes())
	}
	// A 12-byte arena reference and two 4-byte index slots per possible group keep
	// the open-addressed index at most half full. The old table and states stay
	// charged until the replacement has been allocated and copied. Eight extra
	// bytes cover allocator rounding for the smallest reference slice.
	const tableSlotBytes int64 = 20
	if int64(capacity) > math.MaxInt64/(tableSlotBytes+lanes*8) {
		return fmt.Errorf("%w: aggregate group capacity overflows", vectorized.ErrMemoryBudgetExceeded)
	}
	newTableBytes := int64(capacity)*tableSlotBytes + 8
	newStateBytes := int64(capacity) * lanes * 8
	if err := g.tracker.Reserve(newTableBytes + newStateBytes); err != nil {
		return err
	}
	g.metrics.reserve(newTableBytes + newStateBytes)
	keys := make([]groupKey, len(g.keys), capacity)
	copy(keys, g.keys)
	index := make([]uint32, capacity*2)
	mask := uint64(len(index) - 1)
	for id := range keys {
		position := xxhash.Sum64String(g.key(id)) & mask
		for index[position] != 0 {
			position = (position + 1) & mask
		}
		index[position] = uint32(id) + 1
	}
	for index := range states {
		states[index].grow(capacity)
	}
	oldBytes := g.tableBytes + int64(g.capacity)*lanes*8
	g.tracker.Release(oldBytes)
	g.metrics.release(oldBytes)
	g.index, g.keys, g.capacity, g.tableBytes = index, keys, capacity, newTableBytes
	return nil
}

func (g *aggregateGroups) close() {
	for _, chunk := range g.chunks {
		if cap(chunk.data) == groupKeyChunkBytes {
			chunk.data = chunk.data[:0]
			groupKeyChunkPool.Put(chunk)
		}
	}
	g.chunks = nil
	bytes := g.tableBytes + g.keyReserved + g.scratchBytes
	if bytes != 0 {
		g.tracker.Release(bytes)
		g.metrics.release(bytes)
	}
	g.index, g.keys, g.scratch = nil, nil, nil
	g.capacity, g.tableBytes, g.keyBytes, g.keyReserved, g.scratchBytes = 0, 0, 0, 0, 0
}

func decodeGroupValues(key string, values []aggregateValue) {
	position := 0
	for index := range values {
		value := aggregateValue{typ: vectorized.ColumnType(key[position] & 0x7f), timestamp: key[position]&0x80 != 0, null: key[position+1] == 0}
		position += 2
		if !value.null {
			number := groupUint64(key[position : position+8])
			position += 8
			switch value.typ {
			case vectorized.ColumnTypeInt64:
				value.i64 = int64(number)
			case vectorized.ColumnTypeFloat64:
				value.f64 = math.Float64frombits(number)
			case vectorized.ColumnTypeString, vectorized.ColumnTypeBytes:
				// Byte output conversion happens only at egress; retaining this string
				// keeps the key-owned bytes immutable until operator Close.
				value.text = key[position : position+int(number)]
				position += int(number)
			default:
				panic("unsupported encoded QL group type")
			}
		}
		values[index] = value
	}
}

func groupUint64(value string) uint64 {
	return uint64(value[0]) | uint64(value[1])<<8 | uint64(value[2])<<16 | uint64(value[3])<<24 |
		uint64(value[4])<<32 | uint64(value[5])<<40 | uint64(value[6])<<48 | uint64(value[7])<<56
}
