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

package grpc

import (
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func (c *compiledPlanCache) validate(dependencies []measurePlanDependency) bool {
	started := time.Now()
	defer c.observeDuration("validate", started)
	return c.source.validateMeasureDependencies(dependencies)
}

func (c *compiledPlanCache) observeDuration(stage string, started time.Time) {
	if c.metrics != nil {
		c.metrics.bydbqlPlanDuration.Observe(time.Since(started).Seconds(), stage)
	}
}

func (c *compiledPlanCache) emitMetrics() {
	if c.metrics == nil {
		return
	}
	before, after := c.reported, c.stats
	for _, event := range []struct {
		name          string
		before, after uint64
	}{
		{"hit", before.Hits, after.Hits},
		{"miss", before.Misses, after.Misses},
		{"stale", before.Stale, after.Stale},
		{"bypass", before.Bypass, after.Bypass},
		{"oversize", before.Oversize, after.Oversize},
		{"coalesced", before.Coalesced, after.Coalesced},
		{"error", before.Errors, after.Errors},
		{"eviction", before.Evictions, after.Evictions},
		{"canceled", before.CanceledCompiles, after.CanceledCompiles},
	} {
		if event.after > event.before {
			c.metrics.bydbqlPlanCacheTotal.Inc(float64(event.after-event.before), event.name)
		}
	}
	if after.Compiles > before.Compiles {
		c.metrics.bydbqlPlanCompiles.Inc(float64(after.Compiles - before.Compiles))
	}
	c.metrics.bydbqlPlanCacheCount.Set(float64(after.Entries))
	c.metrics.bydbqlPlanCacheBytes.Set(float64(after.EntryBytes), "entries")
	c.metrics.bydbqlPlanCacheBytes.Set(float64(after.RetainedBytes), "retained")
	c.metrics.bydbqlPlanCacheBytes.Set(float64(after.CompileReservedBytes), "compile")
	c.metrics.bydbqlPlanCacheBytes.Set(float64(after.UncachedBytes), "uncached")
	c.metrics.bydbqlPlanWork.Set(float64(after.Waiters), "waiters")
	c.metrics.bydbqlPlanWork.Set(float64(after.InFlight), "in_flight")
}

// Saturation preserves rejection rather than wrapping a huge estimate into a
// small cache charge. A successful budget reservation is always representable.
func planCacheSize(base int, extra ...uint64) int64 {
	const maximum = uint64(1<<63 - 1)
	size := uint64(base)
	for _, bytes := range extra {
		if size > maximum || bytes > maximum-size {
			return int64(maximum)
		}
		size += bytes
	}
	return int64(size)
}

func planFlightCost(statement *bydbql.PreparedStatement, key string) int64 {
	return planCacheSize(statement.EstimatedSize(), uint64(len(key)), 256)
}
