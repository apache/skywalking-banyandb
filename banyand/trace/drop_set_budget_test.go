// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
)

// fixedLimitProtector reports a fixed, test-chosen memory limit so
// resolveDropSetBudget's limit-derived path can be exercised without a real
// cgroup.
type fixedLimitProtector struct {
	protector.Nop
	limit uint64
}

func (p fixedLimitProtector) GetLimit() uint64 { return p.limit }

// TestResolveDropSetBudget tables (limit, CPUs) across the values spec section
// 3.4's table is built from, and asserts the exact resulting budget: the
// zero-limit fallback, the per-CPU division, and the minimum clamp.
func TestResolveDropSetBudget(t *testing.T) {
	tests := []struct {
		name   string
		limit  uint64
		cpus   int
		expect uint64
	}{
		{name: "zero limit, 1 cpu", limit: 0, cpus: 1, expect: defaultDropSetBudget},
		{name: "zero limit, 16 cpus", limit: 0, cpus: 16, expect: defaultDropSetBudget},
		{name: "512MiB, 1 cpu", limit: 512 << 20, cpus: 1, expect: (512 << 20) / dropSetAggregateDivisor},
		{name: "512MiB, 4 cpus", limit: 512 << 20, cpus: 4, expect: (512 << 20) / (dropSetAggregateDivisor * 4)},
		{name: "512MiB, 8 cpus", limit: 512 << 20, cpus: 8, expect: (512 << 20) / (dropSetAggregateDivisor * 8)},
		{name: "512MiB, 16 cpus", limit: 512 << 20, cpus: 16, expect: (512 << 20) / (dropSetAggregateDivisor * 16)},
		{name: "4GiB, 1 cpu", limit: 4 << 30, cpus: 1, expect: (4 << 30) / dropSetAggregateDivisor},
		{name: "4GiB, 4 cpus", limit: 4 << 30, cpus: 4, expect: (4 << 30) / (dropSetAggregateDivisor * 4)},
		{name: "4GiB, 8 cpus", limit: 4 << 30, cpus: 8, expect: (4 << 30) / (dropSetAggregateDivisor * 8)},
		{name: "4GiB, 16 cpus", limit: 4 << 30, cpus: 16, expect: (4 << 30) / (dropSetAggregateDivisor * 16)},
		{name: "16GiB, 1 cpu", limit: 16 << 30, cpus: 1, expect: (16 << 30) / dropSetAggregateDivisor},
		{name: "16GiB, 4 cpus", limit: 16 << 30, cpus: 4, expect: (16 << 30) / (dropSetAggregateDivisor * 4)},
		{name: "16GiB, 8 cpus", limit: 16 << 30, cpus: 8, expect: (16 << 30) / (dropSetAggregateDivisor * 8)},
		{name: "16GiB, 16 cpus", limit: 16 << 30, cpus: 16, expect: (16 << 30) / (dropSetAggregateDivisor * 16)},
		// A known but tiny limit still clamps up to the minimum floor.
		{name: "tiny limit, 16 cpus", limit: 1 << 10, cpus: 16, expect: minimumDropSetBudget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, dropSetBudgetFromLimit(tt.limit, tt.cpus))
		})
	}
}

// TestDropSetAggregateBoundHolds asserts the aggregate invariant from spec
// section 3.4, assuming CPUs/2+2 concurrent merges (the hot lanes plus the
// finalize round) all simultaneously at the ceiling:
//
//   - budget*(CPUs/2+2) never exceeds the hard outer bound limit/8.
//   - At CPUs>=4 it stays within the nominal bound limit/dropSetAggregateDivisor
//     (limit/16) that the divisor is chosen for.
//   - Below four CPUs the CPUs divisor is smaller than CPUs/2+2, so the
//     aggregate exceeds the nominal limit/16 bound — asserted explicitly so
//     that documented small-node overshoot doesn't look like a regression.
func TestDropSetAggregateBoundHolds(t *testing.T) {
	limits := []uint64{512 << 20, 4 << 30, 16 << 30}
	cpuCounts := []int{1, 2, 4, 8, 16}
	for _, limit := range limits {
		for _, cpus := range cpuCounts {
			t.Run(fmt.Sprintf("limit=%d/cpus=%d", limit, cpus), func(t *testing.T) {
				budget := dropSetBudgetFromLimit(limit, cpus)
				concurrentMerges := uint64(cpus/2 + 2)
				aggregate := budget * concurrentMerges
				outerBound := limit / 8
				nominalBound := limit / dropSetAggregateDivisor
				require.LessOrEqual(t, aggregate, outerBound, "the aggregate must never exceed the hard outer bound")
				if cpus < 4 {
					require.Greater(t, aggregate, nominalBound,
						"cpus=%d is documented to overshoot the nominal limit/%d bound", cpus, dropSetAggregateDivisor)
					return
				}
				require.LessOrEqual(t, aggregate, nominalBound)
			})
		}
	}
}

// TestDropSetFloorBoundRegime pins what minimumDropSetBudget does when it binds,
// which the limits in TestDropSetAggregateBoundHolds are all too large to reach.
// The floor binds when limit < 16MB*CPUs. Above a plausible smallest data-node
// limit the aggregate bound still holds; below it the floor dominates, which is
// accepted rather than silently true.
func TestDropSetFloorBoundRegime(t *testing.T) {
	const plausibleSmallestLimit = 128 << 20
	for _, tt := range []struct {
		limit uint64
		cpus  int
	}{
		{limit: 128 << 20, cpus: 8},
		{limit: 128 << 20, cpus: 16},
		{limit: 32 << 20, cpus: 16},
		{limit: 16 << 20, cpus: 16},
	} {
		t.Run(fmt.Sprintf("limit=%d/cpus=%d", tt.limit, tt.cpus), func(t *testing.T) {
			budget := dropSetBudgetFromLimit(tt.limit, tt.cpus)
			require.Equal(t, uint64(minimumDropSetBudget), budget, "the floor is expected to bind at this limit and CPU count")
			aggregate := budget * uint64(tt.cpus/2+2)
			if tt.limit >= plausibleSmallestLimit {
				require.LessOrEqual(t, aggregate, tt.limit/8)
				return
			}
			// Documented and accepted: on an implausibly small limit the floor, not
			// the divisor, governs, so the aggregate is a large share of the limit.
			// The floor stays because a sub-megabyte ceiling would make sampling
			// useless; see minimumDropSetBudget's comment.
			require.Greater(t, aggregate, tt.limit/8)
		})
	}
}

// TestDropSetBudgetOverrideWins asserts the test-only override short-circuits
// resolveDropSetBudget regardless of the protector limit.
func TestDropSetBudgetOverrideWins(t *testing.T) {
	testDropSetBudgetOverride = 12345
	t.Cleanup(func() { testDropSetBudgetOverride = 0 })

	opt := option{protector: fixedLimitProtector{limit: 4 << 30}}
	require.Equal(t, uint64(12345), resolveDropSetBudget(opt))

	opt = option{protector: protector.Nop{}}
	require.Equal(t, uint64(12345), resolveDropSetBudget(opt))
}
