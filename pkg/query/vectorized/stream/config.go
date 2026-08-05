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

package stream

import (
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// VectorizedConfig controls the v1 vectorized Stream query path.
type VectorizedConfig struct {
	BatchSize int
	// QueryMemoryMiB is a soft element-loading threshold: it caps the cumulative
	// uncompressed element bytes fetched from disk per query. Tags, record-batch
	// overhead, and other per-query allocations are not counted. The first block
	// always loads regardless of the budget (first-block exception), so a single
	// oversized block may exceed this value.
	QueryMemoryMiB int
	Enabled        bool
}

// DefaultConfig returns the default stream vectorized configuration — enabled,
// with the shared default batch size and a 256 MiB per-query memory budget.
//
// The query layer dedups by element_id and holds the seen-set for the whole query,
// because an element_id identifies an element GLOBALLY: two rows carrying one
// element_id are one element, whatever their timestamps or which parts they were
// read from. test/cases/stream's "deduplication test" pins that — 50 records over 27
// distinct ids at 50 different timestamps must come back as 27 rows.
//
// The row path uses the same key but allocates its seen-set per merge round
// (blockCursorHeap.merge / model.MergeStreamResults, one per runTabScanner call), so
// it only collapses duplicates that land in the same round. That is a weaker
// guarantee than this path gives, not a different semantic; a fixture that reuses an
// element_id across two writes is malformed either way.
//
// Enabled also selects the liaison<->data wire format: a flag-on distributed data
// node emits the native columnar frame instead of protobuf. A liaison decodes both
// (it dispatches on the frame magic byte per message), but an older liaison has no
// frame decoder at all, so a cluster must upgrade liaison nodes BEFORE data nodes.
// See docs/operation/upgrade.md.
//
// To roll back the vec path entirely, pass --stream-vectorized-enabled=false on the
// standalone or data-node command line and restart; the row path resumes immediately.
func DefaultConfig() VectorizedConfig {
	return VectorizedConfig{
		Enabled:        true,
		BatchSize:      vectorized.DefaultBatchSize,
		QueryMemoryMiB: 256,
	}
}

// Validate rejects invalid stream vectorized configurations.
func (c VectorizedConfig) Validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("vectorized.stream: BatchSize must be > 0, got %d", c.BatchSize)
	}
	// A batch's Selection is []uint16, so a batch cannot hold more than
	// math.MaxUint16 rows without overflowing the selection index.
	if c.BatchSize > math.MaxUint16 {
		return fmt.Errorf("vectorized.stream: BatchSize must be <= %d, got %d", math.MaxUint16, c.BatchSize)
	}
	if c.QueryMemoryMiB <= 0 {
		return fmt.Errorf("vectorized.stream: QueryMemoryMiB must be > 0, got %d", c.QueryMemoryMiB)
	}
	return nil
}
