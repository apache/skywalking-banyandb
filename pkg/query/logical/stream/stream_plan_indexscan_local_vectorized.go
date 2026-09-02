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
// applies the per-element tagFilter.Match + hidden-tag strip at egress (via
// VecTagFilter). A multi-group merger or any other shape does not resolve to a
// *localIndexScan, so we decline and the caller fails the query.
//
// An index-order query need not project its ordered tag: vecTagProjection adds it
// to the scan's request and keeps it out of ProjectionTags().
func VecExecutable(plan logical.Plan) executor.StreamVecExecutable {
	l, ok := plan.(*limit)
	if !ok {
		return nil
	}
	scan := scanFromInput(l.Input)
	if scan == nil {
		return nil
	}
	if _, _, resolved := scan.vecTagProjection(); !resolved {
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
			// A criteria query applies the tag filter at egress, AFTER the scan, so the
			// vec merge must reproduce exactly the element set the row scan would hand
			// its tagFilterPlan. Where the row scan caps depends on the order type —
			// see scanResumesAcrossPulls.
			scan.deferLimitToEgress = scanResumesAcrossPulls(scan)
			return scan
		}
	}
	return nil
}

// scanResumesAcrossPulls reports whether the storage scan backing this plan keeps
// yielding new elements on successive Pulls, which decides where the vec merge may
// cap for a criteria (filtered) query.
//
// The rule is inherited from the row path removed in 0.12.0, which nested three
// loops: the limit pulled the tag filter until it had accumulated limit+offset
// elements, the tag filter pulled the scan until a batch yielded ≥1 match, and the
// scan itself capped each batch at maxElementSize. Whether the limit actually got
// FILLED therefore depended on the scan resuming:
//
//   - index-order (idxResult): the sorted iterator persists across Pulls and each
//     Pull drains the next maxElementSize entries (query_by_idx.go:262), so row kept
//     pulling and DID fill the limit. The vec merge must stay uncapped, letting the
//     egress filter the whole ordered set and then apply the limit.
//   - timestamp order (tsResult): one Pull consumes a whole segment and caps the
//     result at maxElementSize (query_by_ts.go:136,159); the next Pull only advances
//     to a further segment. For data inside a single segment the scan was then
//     exhausted, so row returned only the matches from that first capped batch and
//     legitimately UNDER-filled the limit (e.g. 30 scanned, 2 rejected ⇒ 28 returned).
//     The vec merge must cap at maxElementSize to reproduce that same input set.
//
// Behaviour change in 0.12.0 (documented, not emulated): for timestamp order spanning
// MULTIPLE segments the row scan resumed per segment, so it could accumulate past the
// first capped batch where vec stops. Reproducing that needs the scan's segment
// boundaries, which the vec merge does not see, so vec's answer is now the only
// answer. See docs/operation/upgrade.md.
func scanResumesAcrossPulls(scan *localIndexScan) bool {
	return scan.order != nil && scan.order.Index != nil
}

// VecTagFilter returns the criteria tag filter, the hidden-tag set, and the schema
// carried by the *limit plan's *tagFilterPlan input, so the standalone vec egress
// applies the per-element tagFilter.Match + hidden-tag strip at egress. Returns ok=false when the plan is not the
// *limit → *tagFilterPlan shape (a criteria-less query needs no post-filter).
func VecTagFilter(plan logical.Plan) (tagFilter logical.TagFilter, hiddenTags logical.HiddenTagSet, schema logical.Schema, ok bool) {
	l, isLimit := plan.(*limit)
	if !isLimit {
		return nil, nil, nil, false
	}
	return nodeTagFilter(l.Input)
}

// nodeTagFilter returns the per-node tag filter, hidden-tag set, and schema when
// plan is a *tagFilterPlan (a criteria group), else ok=false. It operates on a
// plan NODE directly (not the *limit wrapper), so both the single-group VecTagFilter
// (via l.Input) and the multi-group dispatch (via each mergePlan subPlan) share the
// same extraction.
func nodeTagFilter(plan logical.Plan) (tagFilter logical.TagFilter, hiddenTags logical.HiddenTagSet, schema logical.Schema, ok bool) {
	tf, isTagFilter := plan.(*tagFilterPlan)
	if !isTagFilter {
		return nil, nil, nil, false
	}
	return tf.tagFilter, tf.hiddenTags, tf.s, true
}

// VecMergeGroup is one group's resolved vec scan plus its optional per-element
// tag filter, for the multi-group dispatch. The processor runs Scan.ExecuteVectorized
// → BuildElementsFromBatches → (if HasFilter) applyStreamTagFilter, yielding that
// group's ordered []Element; the caller then cross-group merges via MergeGroupElements.
type VecMergeGroup struct {
	Scan         executor.StreamVecExecutable
	TagFilter    logical.TagFilter
	HiddenTags   logical.HiddenTagSet
	FilterSchema logical.Schema
	HasFilter    bool
}

// VecMerge is the vec-eligible form of a multi-group query (*limit → *mergePlan).
// It carries each group's resolved vec node plus the EXACT merge params the row
// mergePlan.Execute uses (SortByTime, SortTagSpec, Desc), so the dispatch merges
// across groups via the shared MergeGroupElements and slices with Offset/Limit.
type VecMerge struct {
	SortTagSpec logical.TagSpec
	Groups      []VecMergeGroup
	Offset      uint32
	Limit       uint32
	SortByTime  bool
	Desc        bool
}

// VecMergeExecutable returns the vec-eligible multi-group form when the plan is
// *limit → *mergePlan and EVERY subPlan resolves to a vec-eligible *localIndexScan
// (via scanFromInput). If ANY subPlan is not vec-eligible it returns ok=false and
// the caller fails the whole query. The merge params (sortByTime/sortTagSpec/desc)
// are taken verbatim from the mergePlan so the cross-group order is unchanged.
func VecMergeExecutable(plan logical.Plan) (*VecMerge, bool) {
	l, isLimit := plan.(*limit)
	if !isLimit {
		return nil, false
	}
	mp, isMerge := l.Input.(*mergePlan)
	if !isMerge {
		return nil, false
	}
	groups := make([]VecMergeGroup, 0, len(mp.subPlans))
	for _, sp := range mp.subPlans {
		scan := scanFromInput(sp)
		if scan == nil {
			return nil, false
		}
		if _, _, resolved := scan.vecTagProjection(); !resolved {
			return nil, false
		}
		filter, hidden, filterSchema, hasFilter := nodeTagFilter(sp)
		groups = append(groups, VecMergeGroup{
			Scan:         scan,
			TagFilter:    filter,
			HiddenTags:   hidden,
			FilterSchema: filterSchema,
			HasFilter:    hasFilter,
		})
	}
	return &VecMerge{
		Groups:      groups,
		SortByTime:  mp.sortByTime,
		SortTagSpec: mp.sortTagSpec,
		Desc:        mp.desc,
		Offset:      l.offsetNum,
		Limit:       l.limitNum,
	}, true
}

// vecTagProjection returns the tag projection the vec scan requests from storage.
// The vec merge keys on the OrderKey column, which the scan derives from the
// ordered tag's projected cell (resolveOrderTag); an index-order query that does
// not project its ordered tag would otherwise get an empty OrderKey column and
// silently sort by timestamp. So the ordered tag is appended here and reported as
// hidden — ProjectionTags() still returns the client projection, so the extra tag
// never reaches the element egress.
//
// resolved is false when the ordered tag has to be added but its family cannot be
// resolved against the schema (a stale index rule naming a dropped tag). The
// caller then declines vec, because the alternative is a silent timestamp sort.
func (i *localIndexScan) vecTagProjection() (projection []model.TagProjection, hidden, resolved bool) {
	if i.order == nil || i.order.Index == nil {
		return i.projectionTags, false, true
	}
	tags := i.order.Index.GetTags()
	if len(tags) != 1 {
		return i.projectionTags, false, true
	}
	name := tags[0]
	for _, proj := range i.projectionTags {
		for _, projName := range proj.Names {
			if projName == name {
				return i.projectionTags, false, true
			}
		}
	}
	tagSpec := i.schema.FindTagSpecByName(name)
	if tagSpec == nil {
		return i.projectionTags, false, false
	}
	family, ok := familyNameFromSchema(i.schema, tagSpec)
	if !ok {
		return i.projectionTags, false, false
	}
	augmented := make([]model.TagProjection, 0, len(i.projectionTags)+1)
	appended := false
	for _, proj := range i.projectionTags {
		if proj.Family == family && !appended {
			names := make([]string, 0, len(proj.Names)+1)
			proj.Names = append(append(names, proj.Names...), name)
			appended = true
		}
		augmented = append(augmented, proj)
	}
	if !appended {
		augmented = append(augmented, model.TagProjection{Family: family, Names: []string{name}})
	}
	return augmented, true, true
}

// HidesOrderTag implements executor.StreamVecExecutable.
func (i *localIndexScan) HidesOrderTag() bool {
	_, hidden, _ := i.vecTagProjection()
	return hidden
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
	tagProjection, _, _ := i.vecTagProjection()
	source, err := i.ec.QueryVectorized(ctx, model.StreamQueryOptions{
		Name:           i.metadata.GetName(),
		TimeRange:      &i.timeRange,
		Entities:       i.entities,
		InvertedFilter: i.invertedFilter,
		SkippingFilter: i.skippingFilter,
		Order:          orderBy,
		TagProjection:  tagProjection,
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
	//
	// This path is therefore NOT bounded by limit+offset, and cannot be until the
	// tag filter moves ahead of the merge. Any cap here is unsound in general: the
	// filter's selectivity is unknown, so the top-(limit+offset) rows BEFORE it can
	// contain arbitrarily few surviving rows — including zero — while the row path
	// keeps pulling until it has enough. Bounding it needs columnar tag-filter
	// pushdown (out of scope here), not a bigger cap.
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
