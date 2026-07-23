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
	"math"

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
// must resolve to a *localIndexScan — either directly (no criteria) or wrapped in a
// *tagFilterPlan (criteria query, stream_plan_tag_filter.go). For the tag-filter
// case the inner scan already projects the criteria + hidden tags and pushes the
// INDEXED criteria into its sqo (invertedFilter/skippingFilter); the caller then
// applies the SAME per-element tagFilter.Match + hidden-tag strip that
// tagFilterPlan.Execute does (via VecTagFilter) so the result is byte-identical to
// the row path. A multi-group merger or any other shape does not resolve to a
// *localIndexScan, so we decline and the caller runs the row path.
func VecExecutable(plan logical.Plan) executor.StreamVecExecutable {
	l, ok := plan.(*limit)
	if !ok {
		return nil
	}
	scan := scanFromInput(l.Input)
	if scan == nil {
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

// scanFromInput resolves the *localIndexScan at the input of the *limit node,
// unwrapping a *tagFilterPlan (criteria query) whose parent is the scan. Returns
// nil for any other shape (e.g. a multi-group merger).
func scanFromInput(input logical.Plan) *localIndexScan {
	switch in := input.(type) {
	case *localIndexScan:
		return in
	case *tagFilterPlan:
		if scan, ok := in.parent.(*localIndexScan); ok {
			// A criteria query applies the tag filter at egress AFTER the scan; the vec
			// merge must not cap at maxElementSize or it would starve the filter (see
			// localIndexScan.deferLimitToEgress).
			scan.deferLimitToEgress = true
			return scan
		}
	}
	return nil
}

// VecTagFilter returns the criteria tag filter, the hidden-tag set, and the schema
// carried by the *limit plan's *tagFilterPlan input, so the standalone vec egress
// can apply the SAME per-element tagFilter.Match + hidden-tag strip that the row
// path's tagFilterPlan.Execute applies. Returns ok=false when the plan is not the
// *limit → *tagFilterPlan shape (a criteria-less query needs no post-filter).
func VecTagFilter(plan logical.Plan) (tagFilter logical.TagFilter, hiddenTags logical.HiddenTagSet, schema logical.Schema, ok bool) {
	l, isLimit := plan.(*limit)
	if !isLimit {
		return nil, nil, nil, false
	}
	tf, isTagFilter := l.Input.(*tagFilterPlan)
	if !isTagFilter {
		return nil, nil, nil, false
	}
	return tf.tagFilter, tf.hiddenTags, tf.s, true
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
// offset:offset+limit slice at egress. For a criteria-less query the vec pipeline
// caps at maxElementSize (offset 0, limit=maxElementSize) exactly like the
// scan-level cap, and the outer limit node trims — no double offset. For a criteria
// query (deferLimitToEgress) the merge runs UNCAPPED and the egress applies the tag
// filter before the outer offset:offset+limit slice, so the filter is never starved
// by a premature cap (row-path parity — the row scan streams the whole ordered set).
func (i *localIndexScan) ExecuteVectorized(ctx context.Context) ([]*vectorized.RecordBatch, *vectorized.BatchSchema, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	// Reaching ExecuteVectorized means the dispatch chose the vec path for this
	// query (VecExecutable returned non-nil). Count it so integration tests can
	// assert the vec path actually fired rather than silently falling back to row.
	vstream.IncrQueryCount()

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
	// A criteria query applies a tag filter at egress AFTER this merge. Capping the
	// merge at maxElementSize here would keep only the top-N BEFORE the filter runs
	// and starve it (the row path streams the whole ordered set and filters lazily).
	// So defer the cap to the egress: run the merge UNCAPPED (mergeCap 0) and make the
	// trailing pipeline Limit a pass-through (MaxUint32, since a 0 limit emits
	// NOTHING) — the egress then applies the tag filter and the true
	// offset:offset+limit slice.
	limitRows := uint32(0)
	mergeCap := i.maxElementSize
	if i.deferLimitToEgress {
		mergeCap = 0
		limitRows = math.MaxUint32
	} else if i.maxElementSize > 0 {
		limitRows = uint32(i.maxElementSize)
	}

	pipeline, buildErr := vstream.BuildStreamMergePipeline(
		&vecSourceOperator{source: source, schema: schema},
		schema, desc, 0, limitRows, batchSize, mergeCap)
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
