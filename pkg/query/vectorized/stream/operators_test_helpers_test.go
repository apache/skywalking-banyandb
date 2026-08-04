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

package stream

import (
	"context"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const testTagFamily = "searchable"

const testTagName = "service"

// testRow describes one logical stream row for building test batches.
type testRow struct {
	tag      string
	orderKey []byte
	ts       int64
	elemID   uint64
}

// tsSchema is the time-order schema (no order-key column).
func tsSchema() *vectorized.BatchSchema {
	return BuildStreamBatchSchema(
		[]model.TagProjection{{Family: testTagFamily, Names: []string{testTagName}}},
		"", "",
	)
}

// idxSchema is the index-order schema (with an order-key bytes column).
func idxSchema() *vectorized.BatchSchema {
	return BuildStreamBatchSchema(
		[]model.TagProjection{{Family: testTagFamily, Names: []string{testTagName}}},
		testTagFamily, testTagName,
	)
}

// buildBatch materializes a RecordBatch of the given schema from testRows.
func buildBatch(schema *vectorized.BatchSchema, rows []testRow) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(schema, len(rows))
	tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	seriesCol := batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	tagIdx, _ := schema.TagIndex(testTagFamily, testTagName)
	tagCol := batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
	orderCol := streamOrderKeys(batch)
	for _, r := range rows {
		tsCol.Append(r.ts)
		elemCol.Append(ElementIDToColumn(r.elemID))
		seriesCol.Append(0)
		tagCol.Append(strTagValue(r.tag))
		if orderCol != nil {
			orderCol.Append(r.orderKey)
		}
		batch.Len++
	}
	return batch
}

// strTagValue wraps a string into a modelv1.TagValue.
func strTagValue(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

// staticBatchSource is a test PullOperator that replays pre-built batches.
type staticBatchSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	pos     int
}

func newStaticBatchSource(schema *vectorized.BatchSchema, batches ...*vectorized.RecordBatch) *staticBatchSource {
	return &staticBatchSource{schema: schema, batches: batches}
}

func (s *staticBatchSource) Init(context.Context) error { return nil }

func (s *staticBatchSource) OutputSchema() *vectorized.BatchSchema { return s.schema }

func (s *staticBatchSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.pos]
	s.pos++
	return batch, nil
}

func (s *staticBatchSource) Close() error { return nil }

// drainRows drains a pipeline into flat slices of (ts, elemID, tag) tuples,
// honoring each output batch's active selection.
func drainRows(t testingT, p *vectorized.Pipeline) (tss []int64, ids []uint64, tags []string) {
	ctx := context.Background()
	if err := p.Init(ctx); err != nil {
		t.Fatalf("pipeline init: %v", err)
	}
	for {
		batch, err := p.Next(ctx)
		if err != nil {
			t.Fatalf("pipeline next: %v", err)
		}
		if batch == nil {
			break
		}
		tsData := streamTimestamps(batch).Data()
		idData := streamElementIDs(batch).Data()
		tagIdx, _ := batch.Schema.TagIndex(testTagFamily, testTagName)
		tagData := batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
		for _, rowIdx := range activeIndices(batch) {
			tss = append(tss, tsData[rowIdx])
			ids = append(ids, ColumnToElementID(idData[rowIdx]))
			tags = append(tags, tagData[rowIdx].GetStr().GetValue())
		}
	}
	return tss, ids, tags
}

// testingT is the minimal testing surface drainRows needs.
type testingT interface {
	Fatalf(format string, args ...any)
}
