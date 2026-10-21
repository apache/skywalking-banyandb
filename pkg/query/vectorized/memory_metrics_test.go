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

package vectorized

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestQueryMemoryMetricsBuildMode(t *testing.T) {
	before := QueryMemoryStats()
	// Exceed the prior peak so this check also proves that peak recording runs.
	reservation := max(int64(64), before.PeakReservedBytes-before.LiveReservedBytes+64)
	lease := &trackerTestLease{limit: uint64(reservation)}
	tracker := newQueryMemoryTracker(query.WithBudgetLease(t.Context(), lease), reservation)
	require.NoError(t, tracker.Reserve(reservation))
	require.Equal(t, reservation, tracker.Used())
	require.Equal(t, uint64(reservation), lease.Used())
	require.ErrorIs(t, tracker.Reserve(1), ErrMemoryBudgetExceeded)
	during := QueryMemoryStats()
	if QueryMetricsEnabled {
		require.Equal(t, before.LiveReservedBytes+reservation, during.LiveReservedBytes)
		require.Equal(t, during.LiveReservedBytes, during.PeakReservedBytes)
		require.Equal(t, before.LeaseChargedBytes+uint64(reservation), during.LeaseChargedBytes)
		require.False(t, ResetQueryMemoryPeak(), "live reservations must prevent peak reset")
	} else {
		require.Equal(t, MemoryCounters{}, before)
		require.Equal(t, MemoryCounters{}, during)
		require.True(t, ResetQueryMemoryPeak())
	}
	tracker.Release(reservation)
	require.Zero(t, tracker.Used())
	after := QueryMemoryStats()
	require.Equal(t, before.LiveReservedBytes, after.LiveReservedBytes)
	require.Equal(t, during.LeaseChargedBytes, after.LeaseChargedBytes)
}
