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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func phase1Keys(batch *vectorized.RecordBatch) *vectorized.TypedColumn[int64] {
	return batch.Columns[phase1ColumnKey].(*vectorized.TypedColumn[int64])
}

func phase1SeriesIDs(batch *vectorized.RecordBatch) *vectorized.TypedColumn[int64] {
	return batch.Columns[phase1ColumnSeriesID].(*vectorized.TypedColumn[int64])
}

func phase1PartIDs(batch *vectorized.RecordBatch) *vectorized.TypedColumn[int64] {
	return batch.Columns[phase1ColumnPartID].(*vectorized.TypedColumn[int64])
}

func phase1Payloads(batch *vectorized.RecordBatch) *vectorized.TypedColumn[[]byte] {
	return batch.Columns[phase1ColumnPayload].(*vectorized.TypedColumn[[]byte])
}

func phase1TraceIDs(batch *vectorized.RecordBatch) *vectorized.TypedColumn[string] {
	return batch.Columns[phase1ColumnTraceID].(*vectorized.TypedColumn[string])
}

// Phase2TraceIDs returns the traceID column from a Phase-2 batch.
func Phase2TraceIDs(batch *vectorized.RecordBatch) *vectorized.TypedColumn[string] {
	return batch.Columns[phase2ColumnTraceID].(*vectorized.TypedColumn[string])
}

// Phase2Keys returns the sort-key column from a Phase-2 batch.
func Phase2Keys(batch *vectorized.RecordBatch) *vectorized.TypedColumn[int64] {
	return batch.Columns[phase2ColumnKey].(*vectorized.TypedColumn[int64])
}

// Phase2Spans returns the span-bytes column from a Phase-2 batch.
func Phase2Spans(batch *vectorized.RecordBatch) *vectorized.TypedColumn[[]byte] {
	return batch.Columns[phase2ColumnSpan].(*vectorized.TypedColumn[[]byte])
}

// Phase2SpanIDs returns the spanID column from a Phase-2 batch.
func Phase2SpanIDs(batch *vectorized.RecordBatch) *vectorized.TypedColumn[string] {
	return batch.Columns[phase2ColumnSpanID].(*vectorized.TypedColumn[string])
}

// Phase2TagCol returns the tag column at the given dynamic index from a Phase-2 batch.
// idx=0 is the first dynamic tag column (the first column after the fixed columns).
func Phase2TagCol(batch *vectorized.RecordBatch, idx int) *vectorized.TypedColumn[*modelv1.TagValue] {
	return batch.Columns[phase2FixedColumnCount+idx].(*vectorized.TypedColumn[*modelv1.TagValue])
}
