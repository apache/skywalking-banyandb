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
	"context"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// traceRowBucket accumulates row data for a single traceID.
// traceID and key are constant across all rows in a group.
type traceRowBucket struct {
	traceID string
	spans   [][]byte
	spanIDs []string
	tagCols [][]*modelv1.TagValue // one slice per tag column
	key     int64
}

// GroupByTraceID is a BreakerOperator that groups Phase-2 rows by traceID
// and emits one RecordBatch per traceID in Phase-1 arrival order.
type GroupByTraceID struct {
	schema        *vectorized.BatchSchema
	knownTraceIDs map[string]struct{}
	buckets       map[string]*traceRowBucket
	traceIDsOrder []string
	emitIdx       int
	numTagCols    int
}

// NewGroupByTraceID constructs the GroupByTraceID BreakerOperator.
// traceIDsOrder is the Phase-1 arrival order used to determine output ordering.
func NewGroupByTraceID(schema *vectorized.BatchSchema, traceIDsOrder []string) *GroupByTraceID {
	numTagCols := max(len(schema.Columns)-phase2FixedColumnCount, 0)
	known := make(map[string]struct{}, len(traceIDsOrder))
	for _, tid := range traceIDsOrder {
		known[tid] = struct{}{}
	}
	return &GroupByTraceID{
		schema:        schema,
		traceIDsOrder: append([]string(nil), traceIDsOrder...),
		knownTraceIDs: known,
		numTagCols:    numTagCols,
	}
}

// Init resets accumulated state.
func (g *GroupByTraceID) Init(context.Context) error {
	g.buckets = make(map[string]*traceRowBucket)
	g.emitIdx = 0
	return nil
}

// OutputSchema returns the Phase-2 schema.
func (g *GroupByTraceID) OutputSchema() *vectorized.BatchSchema {
	return g.schema
}

// Consume accumulates each active row into the bucket for its traceID.
// Rows with traceIDs not in traceIDsOrder are silently dropped.
func (g *GroupByTraceID) Consume(_ context.Context, batch *vectorized.RecordBatch) error {
	tidCol := Phase2TraceIDs(batch)
	keyCol := Phase2Keys(batch)
	spanCol := Phase2Spans(batch)
	spanIDCol := Phase2SpanIDs(batch)

	tids := tidCol.Data()
	keys := keyCol.Data()
	spans := spanCol.Data()
	spanIDs := spanIDCol.Data()
	tagData := make([][]*modelv1.TagValue, g.numTagCols)
	for tagIdx := range g.numTagCols {
		tagData[tagIdx] = Phase2TagCol(batch, tagIdx).Data()
	}

	for _, rowIdx := range activeIndices(batch) {
		traceID := tids[rowIdx]
		if _, ok := g.knownTraceIDs[traceID]; !ok {
			continue
		}
		bucket, exists := g.buckets[traceID]
		if !exists {
			bucket = &traceRowBucket{
				traceID: traceID,
				key:     keys[rowIdx],
				tagCols: make([][]*modelv1.TagValue, g.numTagCols),
			}
			g.buckets[traceID] = bucket
		}
		bucket.spans = append(bucket.spans, spans[rowIdx])
		if int(rowIdx) < len(spanIDs) {
			bucket.spanIDs = append(bucket.spanIDs, spanIDs[rowIdx])
		} else {
			bucket.spanIDs = append(bucket.spanIDs, "")
		}
		for tagIdx := range g.numTagCols {
			var tv *modelv1.TagValue
			if int(rowIdx) < len(tagData[tagIdx]) {
				tv = tagData[tagIdx][rowIdx]
			}
			bucket.tagCols[tagIdx] = append(bucket.tagCols[tagIdx], tv)
		}
	}
	return nil
}

// Finalize is a no-op; all grouping happens in Consume.
func (g *GroupByTraceID) Finalize(context.Context) error {
	return nil
}

// NextBatch emits one RecordBatch per traceID in traceIDsOrder.
// TraceIDs with no accumulated rows are skipped.
// Returns (nil, nil) when all traceIDs have been emitted.
func (g *GroupByTraceID) NextBatch(_ context.Context) (*vectorized.RecordBatch, error) {
	for g.emitIdx < len(g.traceIDsOrder) {
		traceID := g.traceIDsOrder[g.emitIdx]
		g.emitIdx++
		bucket, ok := g.buckets[traceID]
		if !ok || len(bucket.spans) == 0 {
			continue
		}
		batch := vectorized.NewRecordBatch(g.schema, len(bucket.spans))
		tidOut := Phase2TraceIDs(batch)
		keyOut := Phase2Keys(batch)
		spanOut := Phase2Spans(batch)
		spanIDOut := Phase2SpanIDs(batch)
		for rowIdx, span := range bucket.spans {
			tidOut.Append(bucket.traceID)
			keyOut.Append(bucket.key)
			spanOut.Append(span)
			spanIDOut.Append(bucket.spanIDs[rowIdx])
		}
		for tagIdx := range g.numTagCols {
			tagOut := Phase2TagCol(batch, tagIdx)
			for _, tv := range bucket.tagCols[tagIdx] {
				tagOut.Append(tv)
			}
		}
		batch.Len = len(bucket.spans)
		return batch, nil
	}
	return nil, nil
}

// Close releases the accumulated buckets.
func (g *GroupByTraceID) Close() error {
	g.buckets = nil
	return nil
}
