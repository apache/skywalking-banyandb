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
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
)

var _ executor.StreamVecExecutable = (*localIndexScan)(nil)

// VecExecutable returns the vec-eligible StreamVecExecutable at the scan position
// of the analyzed plan, or nil when the plan cannot be vectorized.
//
// Eligibility: the plan top is the *limit node (stream_analyzer.go:86); its Input
// must be a bare *localIndexScan. A tag-filter wrapper (criteria tags needing
// row-side re-matching), a multi-group merger, or any other shape means the Input
// is not a *localIndexScan, so we decline and the caller runs the row path.
func VecExecutable(plan logical.Plan) executor.StreamVecExecutable {
	l, ok := plan.(*limit)
	if !ok {
		return nil
	}
	scan, ok := l.Input.(*localIndexScan)
	if !ok {
		return nil
	}
	// An index-order query sorts by an indexed tag. The vec merge keys on the
	// OrderKey column, which is populated from the ordered tag's PROJECTED cell
	// (resolveOrderTag). If the ordered tag is not in the projection, there is no
	// cell to derive the key from, so the OrderKey column would be empty and the
	// merge would silently fall back to timestamp order — a wrong result. The row
	// path sorts via the inverted index regardless of projection, so decline vec
	// here and let the caller run the row path (correct order).
	if !scan.orderTagProjected() {
		return nil
	}
	return scan
}

// orderTagProjected reports whether an index-order query's single ordered tag is
// present in the scan's tag projection (so the vec OrderKey column can be
// populated). It is true for non-index-order queries (they key on timestamp, no
// ordered tag needed) and for the degenerate order shapes vec does not treat as
// index-order (no Index, or not exactly one ordered tag).
func (i *localIndexScan) orderTagProjected() bool {
	if i.order == nil || i.order.Index == nil {
		return true
	}
	tags := i.order.Index.GetTags()
	if len(tags) != 1 {
		return true
	}
	name := tags[0]
	for _, proj := range i.projectionTags {
		for _, projName := range proj.Names {
			if projName == name {
				return true
			}
		}
	}
	return false
}

// VecOffsetLimit returns the client offset/limit the *limit plan node carries, so
// the standalone vec egress can apply the same offset:offset+limit slice the row
// *limit.Execute would apply. Returns ok=false when plan is not the *limit shape.
func VecOffsetLimit(plan logical.Plan) (offsetNum, limitNum uint32, ok bool) {
	l, isLimit := plan.(*limit)
	if !isLimit {
		return 0, 0, false
	}
	return l.offsetNum, l.limitNum, true
}

// ExecuteVectorized runs the localIndexScan through the native columnar path. It
// builds the SAME model.StreamQueryOptions the row-path Execute passes to
// ec.Query, calls ec.QueryVectorized instead, then drives the M4
// merge → distinct → limit pipeline and drains it into columnar batches.
//
// The offset/limit split matches the row path: the localIndexScan's
// maxElementSize is already limit+offset (PushDownMaxSize at
// stream_analyzer.go:94), and the enclosing *limit node applies the final
// offset:offset+limit slice at egress. So the vec pipeline caps at maxElementSize
// (offset 0, limit=maxElementSize) exactly like the scan-level cap, and the outer
// limit node trims — no double offset.
func (i *localIndexScan) ExecuteVectorized(ctx context.Context) ([]*vectorized.RecordBatch, *vectorized.BatchSchema, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	var orderBy *index.OrderBy
	if i.order != nil {
		orderBy = &index.OrderBy{
			Index: i.order.Index,
			Sort:  i.order.Sort,
		}
	}
	source, err := i.ec.QueryVectorized(ctx, model.StreamQueryOptions{
		Name:           i.metadata.GetName(),
		TimeRange:      &i.timeRange,
		Entities:       i.entities,
		InvertedFilter: i.invertedFilter,
		SkippingFilter: i.skippingFilter,
		Order:          orderBy,
		TagProjection:  i.projectionTags,
		MaxElementSize: i.maxElementSize,
	})
	if err != nil {
		return nil, nil, err
	}
	if source == nil {
		return nil, nil, nil
	}

	// The M4 SortedMerge validates batch.Schema by pointer identity, so the merge
	// pipeline MUST use the exact schema the source stamps on its batches — not a
	// freshly-built one that would be a "foreign" pointer.
	schema := source.Schema()

	cfg := i.ec.VectorizedConfig()
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = vectorized.DefaultBatchSize
	}
	desc := i.order != nil && i.order.Sort == modelv1.Sort_SORT_DESC

	// The per-node request cap (maxElementSize = limit+offset) bounds the total
	// rows the merge produces; it is applied INSIDE the merge (in-order top-N by
	// distinct ElementID) so the cap keeps the correct rows in sort order, matching
	// the row path's cap after its in-order heap merge. The trailing Limit still
	// applies the SAME limit with offset 0 as a defensive client slice; the final
	// client offset:offset+limit slice is the enclosing *limit node's job (row path
	// parity).
	limitRows := uint32(0)
	if i.maxElementSize > 0 {
		limitRows = uint32(i.maxElementSize)
	}

	pipeline, buildErr := vstream.BuildStreamMergePipeline(
		&vecSourceOperator{source: source, schema: schema},
		schema, desc, 0, limitRows, batchSize, i.maxElementSize)
	if buildErr != nil {
		source.Release()
		return nil, nil, buildErr
	}
	defer func() {
		_ = pipeline.Close()
	}()

	if initErr := pipeline.Init(ctx); initErr != nil {
		return nil, nil, initErr
	}

	var batches []*vectorized.RecordBatch
	for {
		batch, nextErr := pipeline.Next(ctx)
		if nextErr != nil {
			return nil, nil, nextErr
		}
		if batch == nil {
			break
		}
		if batch.ActiveLen() == 0 {
			continue
		}
		batches = append(batches, batch)
	}
	return batches, schema, nil
}

// ProjectionTags returns the projected tag families/names for the egress.
func (i *localIndexScan) ProjectionTags() []model.TagProjection {
	return i.projectionTags
}

// vecSourceOperator adapts an executor.StreamVecScanSource (NextBatch/Release)
// into a vectorized.PullOperator so it can drive the M4 pipeline. Init is a
// no-op; Close releases the underlying source exactly once.
type vecSourceOperator struct {
	source executor.StreamVecScanSource
	schema *vectorized.BatchSchema
	closed bool
}

func (o *vecSourceOperator) Init(context.Context) error { return nil }

func (o *vecSourceOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }

func (o *vecSourceOperator) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	return o.source.NextBatch(ctx)
}

func (o *vecSourceOperator) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.source.Release()
	return nil
}
