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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawExecutionStatsCountExaminedResidualRows(t *testing.T) {
	source, slots := rawFixture()
	count := uint32(1)
	op, err := WrapRaw(t.Context(), source, &RawPlan{
		Slots: slots, Residual: true,
		Ranges: []MillisRange{{Begin: 0, End: 1}, {Begin: 2000, End: 4000}}, Offset: 1, Limit: &count,
	})
	require.NoError(t, err)
	require.NoError(t, op.Init(t.Context()))
	before := RawExecutionStats()
	batch, err := op.NextBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, []uint16{2}, batch.Selection)
	after := RawExecutionStats()
	require.EqualValues(t, 1, after.ProjectionCalls-before.ProjectionCalls)
	require.EqualValues(t, 3, after.FilterInputRows-before.FilterInputRows)
	require.EqualValues(t, 2, after.FilterOutputRows-before.FilterOutputRows)
	require.EqualValues(t, 2, after.LimitInputRows-before.LimitInputRows)
	require.EqualValues(t, 1, after.LimitOutputRows-before.LimitOutputRows)
	require.Zero(t, after.ProjectionCopiedValueBytes-before.ProjectionCopiedValueBytes)
	require.NoError(t, op.Close())
}
