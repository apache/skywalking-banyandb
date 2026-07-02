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

package trace

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// VectorizedConfig controls the v1 vectorized Trace query path.
type VectorizedConfig struct {
	BatchSize int
	// QueryMemoryMiB is a soft span-loading threshold: it caps the cumulative
	// uncompressed span bytes fetched from disk per query. SIDX responses, tags,
	// record-batch overhead, and other per-query allocations are not counted.
	// The first block always loads regardless of the budget (first-block exception),
	// so a single oversized block may exceed this value.
	QueryMemoryMiB int
	Enabled        bool
}

// DefaultConfig returns the default trace vectorized configuration.
func DefaultConfig() VectorizedConfig {
	return VectorizedConfig{
		Enabled:        false,
		BatchSize:      vectorized.DefaultBatchSize,
		QueryMemoryMiB: 256,
	}
}

// Validate rejects invalid trace vectorized configurations.
func (c VectorizedConfig) Validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("vectorized.trace: BatchSize must be > 0, got %d", c.BatchSize)
	}
	if c.QueryMemoryMiB <= 0 {
		return fmt.Errorf("vectorized.trace: QueryMemoryMiB must be > 0, got %d", c.QueryMemoryMiB)
	}
	return nil
}
