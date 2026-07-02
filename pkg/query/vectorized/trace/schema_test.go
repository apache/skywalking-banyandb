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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestNewPhase1Schema(t *testing.T) {
	schema := NewPhase1Schema()
	require.Equal(t, phase1ColumnKey, schema.TimestampIndex())
	require.Equal(t, phase1ColumnSeriesID, schema.SeriesIDIndex())
	require.Equal(t, phase1ColumnPartID, schema.ShardIDIndex())

	payloadIdx, ok := schema.TagIndex("", Phase1ColumnNamePayload)
	require.True(t, ok)
	require.Equal(t, phase1ColumnPayload, payloadIdx)
	require.Equal(t, vectorized.ColumnTypeBytes, schema.Columns[payloadIdx].Type)

	traceIDIdx, ok := schema.TagIndex("", Phase1ColumnNameTraceID)
	require.True(t, ok)
	require.Equal(t, phase1ColumnTraceID, traceIDIdx)
	require.Equal(t, vectorized.ColumnTypeString, schema.Columns[traceIDIdx].Type)
}

func TestPhase1ColumnHelpers(t *testing.T) {
	batch := vectorized.NewRecordBatch(NewPhase1Schema(), 1)
	item := NewMergeItem(1, 2, 3, []byte("payload"))
	item.TraceID = "trace-a"
	appendMergeItem(batch, item)

	require.Equal(t, []int64{1}, phase1Keys(batch).Data())
	require.Equal(t, []int64{2}, phase1SeriesIDs(batch).Data())
	require.Equal(t, []int64{3}, phase1PartIDs(batch).Data())
	require.Equal(t, [][]byte{[]byte("payload")}, phase1Payloads(batch).Data())
	require.Equal(t, []string{"trace-a"}, phase1TraceIDs(batch).Data())
}
