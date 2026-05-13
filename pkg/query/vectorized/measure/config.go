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

import "fmt"

// VectorizedConfig controls the v1 vectorized Measure query path.
type VectorizedConfig struct {
	BatchSize      int
	QueryMemoryMiB int
	Enabled        bool
	// AggregationEnabled opens the dispatch gate to GroupBy+Agg requests
	// (G8d.2 ships the schema/storage bridge but parity-preserving
	// aggregation egress is still being engineered). Default false so
	// production keeps routing GroupBy+Agg through the row path until
	// the egress contract matches row-path output byte-for-byte.
	AggregationEnabled bool
}

// DefaultConfig returns the v1 default — enabled, 1024-row batches, 256 MiB
// per-query memory budget, aggregation gate off. v1 ships Enabled true post-
// soak/bench-gate rollout (G5e); AggregationEnabled stays false until the
// vec aggregation egress reaches row-path parity. To roll back the vec
// path entirely, pass --measure-vectorized-enabled=false on the standalone
// or data-node command line and restart.
func DefaultConfig() VectorizedConfig {
	return VectorizedConfig{Enabled: true, BatchSize: 1024, QueryMemoryMiB: 256}
}

// Validate rejects nonsense configurations.
func (c VectorizedConfig) Validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("vectorized.measure: BatchSize must be > 0, got %d", c.BatchSize)
	}
	if c.QueryMemoryMiB <= 0 {
		return fmt.Errorf("vectorized.measure: QueryMemoryMiB must be > 0, got %d", c.QueryMemoryMiB)
	}
	return nil
}
