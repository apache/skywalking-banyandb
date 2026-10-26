//go:build querymetrics

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

package ql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeAggregateMergeReportsIndependentOwnershipAndLinearVisits(t *testing.T) {
	before, localBefore := NativeAggregateExecutionStats(), AggregateExecutionStats()
	stats := validateNativeMergeKernel(t, makeNativeMergeKernelFixture(1024, true))
	after, localAfter := NativeAggregateExecutionStats(), AggregateExecutionStats()
	require.EqualValues(t, 1, after.Executions-before.Executions)
	require.Equal(t, stats.Branches, after.Branches-before.Branches)
	require.Equal(t, stats.InputRows, after.InputRows-before.InputRows)
	require.Equal(t, stats.Groups, after.Groups-before.Groups)
	require.Equal(t, stats.MergeVisits, after.MergeVisits-before.MergeVisits)
	require.Equal(t, stats.StateCapacity, after.StateCapacity-before.StateCapacity)
	require.Equal(t, after.ReservedBytes-before.ReservedBytes, after.ReleasedBytes-before.ReleasedBytes)
	require.Equal(t, before.LiveBytes, after.LiveBytes)
	require.Equal(t, localBefore, localAfter)
}
