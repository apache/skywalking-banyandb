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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package idgen provides a lock-free snowflake ID generator for stream writes.
package idgen

import (
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	epoch int64 = 1609459200000

	nodeBits     = 7
	sequenceBits = 10

	nodeShift      = sequenceBits
	timeStampShift = nodeBits + sequenceBits

	maxNodeID   int64 = (1 << nodeBits) - 1
	maxSequence int64 = (1 << sequenceBits) - 1
)

// Generator produces unique 64-bit snowflake IDs using a lock-free CAS loop.
// Bit layout: [1-bit sign=0] [46-bit ms timestamp] [7-bit node ID] [10-bit sequence].
// The internal state packs lastTime and sequence into a single uint64:
// state = (lastTime << sequenceBits) | sequence.
type Generator struct {
	l      *logger.Logger
	state  atomic.Uint64
	nodeID int64
}

// NewGenerator creates a Generator whose 7-bit node component is derived by hashing the given nodeID string.
func NewGenerator(nodeID string, l *logger.Logger) *Generator {
	nid := int64(convert.HashStr(nodeID) & uint64(maxNodeID))
	return &Generator{
		nodeID: nid,
		l:      l,
	}
}

// NodeID returns the 7-bit node component used by this generator.
func (g *Generator) NodeID() int64 {
	return g.nodeID
}

// NextID returns the next unique 64-bit ID.
func (g *Generator) NextID() uint64 {
	for {
		old := g.state.Load()
		oldTime := int64(old >> sequenceBits)
		oldSeq := int64(old & uint64(maxSequence))

		now := time.Now().UnixMilli() - epoch

		var newTime, newSeq int64
		if now > oldTime {
			newTime = now
			newSeq = 0
		} else {
			// Clock is equal or regressed — use logical clock from state.
			newTime = oldTime
			newSeq = oldSeq + 1
			if newSeq > maxSequence {
				// Sequence exhausted in this ms — advance logical clock.
				newTime++
				newSeq = 0
			}
		}

		newState := (uint64(newTime) << sequenceBits) | uint64(newSeq)
		if g.state.CompareAndSwap(old, newState) {
			if now < oldTime && g.l != nil {
				g.l.Warn().Int64("lastTime", oldTime).Int64("now", now).
					Msg("clock regression detected, using logical clock")
			}
			return uint64((newTime << timeStampShift) | (g.nodeID << nodeShift) | newSeq)
		}
	}
}
