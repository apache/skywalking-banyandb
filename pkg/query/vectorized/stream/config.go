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

// DefaultConfig returns the default stream vectorized configuration.
func DefaultConfig() VectorizedConfig {
	return VectorizedConfig{
		Enabled:        false,
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
