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

// Package executor defines the specifications accessing underlying data repositories.
package executor

import (
	"context"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

// StreamExecutionContext allows retrieving data through the stream module.
//
// QueryVectorized and VectorizedConfig are the vec-path additions: the former
// yields a pull-based columnar scan source (the M3 vecScanSource), the latter
// surfaces the engine-side --stream-vectorized-* configuration so the vec
// dispatch can decide whether the flag is on.
type StreamExecutionContext interface {
	Query(ctx context.Context, opts model.StreamQueryOptions) (model.StreamQueryResult, error)
	QueryVectorized(ctx context.Context, opts model.StreamQueryOptions) (StreamVecScanSource, error)
	VectorizedConfig() vstream.VectorizedConfig
}

// StreamVecScanSource is the pull contract the vec scan source exposes: one
// columnar batch per NextBatch, (nil, nil) at exhaustion, Release to free
// resources. It mirrors banyand/stream's vecScanSource without importing it, so
// the executor package stays free of a data-node dependency cycle.
type StreamVecScanSource interface {
	NextBatch(ctx context.Context) (*vectorized.RecordBatch, error)
	// Schema returns the batch schema the source stamps on every batch it emits.
	// The M4 SortedMerge validates batch.Schema by pointer identity, so the merge
	// pipeline MUST be built with this exact schema, not a freshly-built one.
	Schema() *vectorized.BatchSchema
	Release()
}

// StreamCloser releases the resources a stream plan node holds. It is separate
// from StreamExecutable so a plan tree can be released without asserting the row
// Execute capability.
type StreamCloser interface {
	Close()
}

// StreamExecutable allows querying in the stream schema.
type StreamExecutable interface {
	Execute(context.Context) ([]*streamv1.Element, error)
	StreamCloser
}

// StreamVecExecutable is the optional capability a stream plan node exposes when
// it can be executed through the native columnar (vectorized) path. The data-node
// processor type-asserts the analyzed plan to this interface; a plan shape that
// cannot be vectorized (multi-group merge, skipping filter, tag filter, etc.)
// simply does not implement it, so the assertion fails and the row path runs.
//
// ExecuteVectorized returns the fully merged/deduped/limited columnar batches
// (the M4 pipeline already applied) plus the batch schema, so both the
// standalone egress and the data-node frame emit consume a single columnar
// result.
type StreamVecExecutable interface {
	ExecuteVectorized(ctx context.Context) ([]*vectorized.RecordBatch, *vectorized.BatchSchema, error)
	// ProjectionTags returns the projected tag families/names in projection order,
	// so the egress builds the same tag families/tags the row path would.
	ProjectionTags() []model.TagProjection
}

// MeasureExecutionContext allows retrieving data through the measure module.
type MeasureExecutionContext interface {
	Query(ctx context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error)
}

// MIterator allows iterating in a measure data set.
type MIterator interface {
	Next() bool

	Current() []*measurev1.InternalDataPoint

	Close() error
}

// MeasureExecutable allows querying in the measure schema.
type MeasureExecutable interface {
	Execute(context.Context) (MIterator, error)
}

// DistributedExecutionContext allows retrieving data through the distributed module.
type DistributedExecutionContext interface {
	bus.Broadcaster
	TimeRange() *modelv1.TimeRange
	NodeSelectors() map[string][]string
}

// DistributedExecutionContextKey is the key of distributed execution context in context.Context.
type DistributedExecutionContextKey struct{}

var distributedExecutionContextKeyInstance = DistributedExecutionContextKey{}

// WithDistributedExecutionContext returns a new context with distributed execution context.
func WithDistributedExecutionContext(ctx context.Context, ec DistributedExecutionContext) context.Context {
	return context.WithValue(ctx, distributedExecutionContextKeyInstance, ec)
}

// FromDistributedExecutionContext returns the distributed execution context from context.Context.
func FromDistributedExecutionContext(ctx context.Context) DistributedExecutionContext {
	return ctx.Value(distributedExecutionContextKeyInstance).(DistributedExecutionContext)
}

// TraceExecutionContext allows retrieving data through the trace module.
type TraceExecutionContext interface {
	Query(ctx context.Context, opts model.TraceQueryOptions) (model.TraceQueryResult, error)
}

// TraceExecutable allows querying in the trace schema.
type TraceExecutable interface {
	Execute(context.Context) (iter.Iterator[model.TraceResult], error)
	Close()
}
