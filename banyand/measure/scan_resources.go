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

package measure

import (
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Process-wide counts of what the vectorized scan holds (measure scan path v2,
// step 4d). They exist so that tests and the soak can prove the scan gives
// everything back: each is one atomic add on a path that already does an atomic
// add of its own.
var (
	liveReservedBytes atomic.Int64
	liveDecodeTasks   atomic.Int64
	liveDecodeWorkers atomic.Int64
	queriesStarted    atomic.Int64
)

const (
	cursorPoolName     = "measure-blockCursor"
	queryResultPoolKey = "measure.queryResult"
)

// ScanResources is a snapshot of the resources the measure scan currently holds.
type ScanResources struct {
	// CursorRefs is the number of block cursors checked out of the cursor pool: held
	// by a scan or pinned by a batch that borrows their buffers.
	CursorRefs int
	// OwnerPins is the number of owner references held by batches that borrow
	// decoded memory.
	OwnerPins int64
	// QueryResults is the number of query results not yet released.
	QueryResults int
	// ReservedBytes is the total scan memory reservations across every query.
	ReservedBytes int64
	// DecodeTasks is the number of decode tasks queued or running on a decode pool.
	DecodeTasks int64
	// DecodeWorkers is the number of decode pool worker goroutines running.
	DecodeWorkers int64
}

// ScanResourceSnapshot returns the resources the scan holds right now. The cursor
// and query-result counts are zero in slim builds, where the pools do not count.
func ScanResourceSnapshot() ScanResources {
	refs := pool.AllRefsCount()
	return ScanResources{
		CursorRefs:    refs[cursorPoolName],
		OwnerPins:     vectorized.OutstandingOwnerPins(),
		QueryResults:  refs[queryResultPoolKey],
		ReservedBytes: liveReservedBytes.Load(),
		DecodeTasks:   liveDecodeTasks.Load(),
		DecodeWorkers: liveDecodeWorkers.Load(),
	}
}

// Idle reports whether the scan holds nothing. Decode workers are not counted: a
// pool's workers live as long as the pool.
func (r ScanResources) Idle() bool {
	return r.CursorRefs == 0 && r.OwnerPins == 0 && r.QueryResults == 0 && r.ReservedBytes == 0 && r.DecodeTasks == 0
}

// QueriesStarted returns how many measure queries this process has started. It never
// decreases, so a soak can check that queries keep flowing.
func QueriesStarted() int64 { return queriesStarted.Load() }
