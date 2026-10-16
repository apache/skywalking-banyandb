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
	"context"
	"fmt"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// RawIterator materializes the existing measure response only at final egress.
// It initializes source and takes its Close ownership; native pagination is not reapplied.
func RawIterator(ctx context.Context, source vectorized.PullOperator) (executor.MIterator, error) {
	if source == nil {
		return nil, fmt.Errorf("raw iterator requires a source")
	}
	if err := ctx.Err(); err != nil {
		_ = source.Close()
		return nil, err
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	if raw, ok := source.(*rawOperator); ok {
		tracker = raw.tracker
	}
	if raw, ok := source.(*rawOperator); ok && len(raw.timestampAliases) > 0 {
		bytes, boundErr := rawDescriptorBound(len(source.OutputSchema().Columns), 0, len(raw.timestampAliases))
		if boundErr != nil {
			_ = source.Close()
			return nil, boundErr
		}
		if reserveErr := tracker.Reserve(bytes); reserveErr != nil {
			_ = source.Close()
			return nil, reserveErr
		}
		defs := slices.Clone(source.OutputSchema().Columns)
		for _, index := range raw.timestampAliases {
			defs[index].Type = vectorized.ColumnTypeTagValue
		}
		source = &timestampEgress{
			source: source, schema: vectorized.NewBatchSchema(defs), indices: slices.Clone(raw.timestampAliases), tracker: tracker,
			metadataBytes: bytes, view: vectorized.RecordBatch{Columns: make([]vectorized.Column, 0, len(defs)), Lent: true},
		}
	}
	pipeline, err := vectorized.NewPipelineBuilder().WithMemoryTracker(tracker).From(source).Build()
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	if err = pipeline.Init(ctx); err != nil {
		_ = pipeline.Close()
		return nil, err
	}
	return vmeasure.NewIteratorFromPipeline(vmeasure.WithFinalBatchSerialization(ctx), pipeline, source.OutputSchema(), nil), nil
}

type timestampEgress struct {
	source        vectorized.PullOperator
	schema        *vectorized.BatchSchema
	tracker       *vectorized.MemoryTracker
	indices       []int
	view          vectorized.RecordBatch
	reserved      int64
	metadataBytes int64
	closed        bool
}

func (e *timestampEgress) Init(ctx context.Context) error        { return e.source.Init(ctx) }
func (e *timestampEgress) OutputSchema() *vectorized.BatchSchema { return e.schema }
func (e *timestampEgress) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.view.Columns = nil
	e.view.Schema = nil
	e.schema = nil
	e.indices = nil
	e.release()
	if e.metadataBytes != 0 {
		e.tracker.Release(e.metadataBytes)
		e.metadataBytes = 0
	}
	return e.source.Close()
}

func (e *timestampEgress) release() {
	if e.reserved != 0 {
		e.tracker.Release(e.reserved)
		e.reserved = 0
	}
}

func (e *timestampEgress) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	clear(e.view.Columns)
	e.release()
	batch, err := e.source.NextBatch(ctx)
	if err != nil || batch == nil {
		return batch, err
	}
	// Reserve temporary final cells before allocation. The existing serializer then
	// accounts their retained protobuf graphs in the response's own reservation.
	bytes := int64(batch.Len)*int64(len(e.indices))*192 + int64(len(e.indices))*128
	if err = e.tracker.Reserve(bytes); err != nil {
		return nil, err
	}
	e.reserved = bytes
	e.view.Schema = e.schema
	e.view.Len = batch.Len
	e.view.Selection = batch.Selection
	e.view.Hints = batch.Hints
	e.view.Lent = true
	e.view.Columns = append(e.view.Columns[:0], batch.Columns...)
	for _, index := range e.indices {
		source := batch.Columns[index]
		constant, isConstant := vectorized.AsConst(source)
		var values []int64
		if typed, ok := source.(*vectorized.TypedColumn[int64]); ok {
			values = typed.Data()
		} else if !isConstant || constant.Type() != vectorized.ColumnTypeInt64 {
			return nil, fmt.Errorf("timestamp alias source is not int64")
		}
		column := vectorized.NewColumnForType(vectorized.ColumnTypeTagValue, batch.Len).(*vectorized.TypedColumn[*modelv1.TagValue])
		for row := 0; row < batch.Len; row++ {
			if row%1024 == 0 {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
			}
			if source.IsNull(row) {
				column.AppendNull()
				continue
			}
			var nanos int64
			if isConstant {
				nanos = constant.Int64()
			} else {
				nanos = values[row]
			}
			column.Append(&modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(0, nanos))}})
		}
		e.view.Columns[index] = column
	}
	return &e.view, nil
}
