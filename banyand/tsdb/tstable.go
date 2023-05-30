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

package tsdb

import (
	"io"
	"time"

	"github.com/dgraph-io/badger/v3"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const maxBlockAge = time.Hour

// TSTable is time series table.
type TSTable interface {
	// Put a value with a timestamp/version
	Put(key, val []byte, ts time.Time) error
	// Get a value by its key and timestamp/version
	Get(key []byte, ts time.Time) ([]byte, error)
	// CollectStats collects statistics of the underlying storage.
	CollectStats() *badger.Statistics
	// SizeOnDisk returns the size of the underlying storage.
	SizeOnDisk() int64
	io.Closer
}

// TSTableFactory is the factory of TSTable.
type TSTableFactory interface {
	// NewTSTable creates a new TSTable.
	NewTSTable(bufferLifecycle BlockExpiryTracker, root string, position common.Position, l *logger.Logger) (TSTable, error)
}

// BlockExpiryTracker tracks the expiry of the buffer.
type BlockExpiryTracker struct {
	clock timestamp.Clock
	ttl   time.Time
}

// IsActive checks if the buffer is active.
func (bl *BlockExpiryTracker) IsActive() bool {
	return !bl.clock.Now().After(bl.EndTime())
}

// EndTime returns the end time of the buffer.
func (bl *BlockExpiryTracker) EndTime() time.Time {
	return bl.ttl.Add(maxBlockAge)
}

// BlockExpiryDuration returns the expiry duration of the buffer.
func (bl *BlockExpiryTracker) BlockExpiryDuration() time.Duration {
	return maxBlockAge
}
