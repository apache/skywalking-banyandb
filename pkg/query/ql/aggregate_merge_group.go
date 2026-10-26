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
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func (o *nativeAggregateOperator) encodeKey() error {
	var bytes int64
	for _, value := range o.groupValues {
		size := groupValueBytes(value)
		if size > math.MaxInt64-bytes {
			return fmt.Errorf("%w: native aggregate key size overflows", vectorized.ErrMemoryBudgetExceeded)
		}
		bytes += size
	}
	if bytes > int64(cap(o.scratch)) {
		capacity, err := groupAllocationBytes(max(int64(64), bytes))
		if err != nil {
			return err
		}
		if err = o.reserve(capacity); err != nil {
			return err
		}
		o.scratch = make([]byte, 0, int(capacity))
		o.release(o.scratchSize)
		o.scratchSize = capacity
	}
	o.scratch = o.scratch[:0]
	for _, value := range o.groupValues {
		o.scratch = appendGroupValue(o.scratch, value)
	}
	return nil
}

func (o *nativeAggregateOperator) groupID() (int, error) {
	if err := o.encodeKey(); err != nil {
		return 0, err
	}
	hash := xxhash.Sum64(o.scratch)
	id, slot, found := o.lookup(hash)
	if found {
		return id, nil
	}
	if o.maxGroups != 0 && uint64(len(o.keys)) >= o.maxGroups {
		return 0, fmt.Errorf("%w: native aggregate group limit %d exceeded", vectorized.ErrMemoryBudgetExceeded, o.maxGroups)
	}
	if len(o.keys) == o.capacity {
		if err := o.grow(); err != nil {
			return 0, err
		}
		_, slot, _ = o.lookup(hash)
	}
	keyBytes := int64(len(o.scratch))
	reserved, err := groupAllocationBytes(keyBytes)
	if err != nil {
		return 0, err
	}
	if err = o.reserve(reserved); err != nil {
		return 0, err
	}
	id = len(o.keys)
	o.keys = append(o.keys, string(o.scratch))
	o.index[slot] = uint32(id) + 1
	o.keyBytes += keyBytes
	o.keyCapacity += reserved
	return id, nil
}

func (o *nativeAggregateOperator) lookup(hash uint64) (id, slot int, found bool) {
	if len(o.index) == 0 {
		return 0, 0, false
	}
	mask := uint64(len(o.index) - 1)
	position := hash & mask
	for {
		entry := o.index[position]
		if entry == 0 {
			return 0, int(position), false
		}
		id = int(entry - 1)
		if o.keys[id] == string(o.scratch) {
			return id, int(position), true
		}
		position = (position + 1) & mask
	}
}

func (o *nativeAggregateOperator) grow() error {
	if o.capacity > math.MaxUint32/2 || o.capacity > math.MaxInt/4 {
		return fmt.Errorf("%w: native aggregate group index overflows", vectorized.ErrMemoryBudgetExceeded)
	}
	capacity := max(1, o.capacity*2)
	var lanes int64
	for index := range o.states {
		lanes += int64(o.states[index].lanes())
	}
	stateWidth := lanes*8 + int64(len(o.states))*2
	const tableWidth int64 = 24 // A key header and two uint32 index slots per group.
	if int64(capacity) > math.MaxInt64/(tableWidth+stateWidth) {
		return fmt.Errorf("%w: native aggregate group capacity overflows", vectorized.ErrMemoryBudgetExceeded)
	}
	tableBytes := int64(capacity) * tableWidth
	// Presence slices smaller than eight bytes still use an eight-byte Go allocation.
	stateBytes := int64(capacity)*lanes*8 + int64(len(o.states))*max(int64(8), int64(capacity)*2)
	if err := o.reserve(tableBytes + stateBytes); err != nil {
		return err
	}
	keys := make([]string, len(o.keys), capacity)
	copy(keys, o.keys)
	index := make([]uint32, capacity*2)
	mask := uint64(len(index) - 1)
	for id, key := range keys {
		position := xxhash.Sum64String(key) & mask
		for index[position] != 0 {
			position = (position + 1) & mask
		}
		index[position] = uint32(id) + 1
	}
	for state := range o.states {
		o.states[state].grow(capacity)
		presence := make([]uint16, capacity)
		copy(presence, o.states[state].presence)
		o.states[state].presence = presence
	}
	o.release(o.tableBytes + o.stateBytes)
	o.keys, o.index, o.capacity = keys, index, capacity
	o.tableBytes, o.stateBytes = tableBytes, stateBytes
	return nil
}
