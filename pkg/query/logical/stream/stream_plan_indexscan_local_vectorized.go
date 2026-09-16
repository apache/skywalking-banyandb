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
// must resolve to a *localIndexScan — either directly (no criteria) or wrapped in a
// *tagFilterPlan (criteria query, stream_plan_tag_filter.go). For the tag-filter
// case the inner scan already projects the criteria + hidden tags and pushes the
// INDEXED criteria into its sqo (invertedFilter/skippingFilter); the caller then
// applies the per-element tagFilter.Match + hidden-tag strip at egress (via
// VecTagFilter). Results match the filter-first contract for duplicate ElementIDs
// — see BuildStreamMergePipeline. A multi-group merger or any other shape does not
// resolve to a *localIndexScan, so we decline and the caller fails the query.
//
// An index-order query need not project its ordered tag: vecTagProjection adds it
// to the scan's request and keeps it out of ProjectionTags().
func VecExecutable(plan logical.Plan) executor.StreamVecExecutable {
	scan, _ := vecExecutable(plan)
	if scan == nil {
		return nil
	}
	return scan
}

// VecDeclineReason explains why VecExecutable rejected a plan, for the error the
// caller returns to the client. It is empty when the plan is vec-eligible.
func VecDeclineReason(plan logical.Plan) string {
	_, reason := vecExecutable(plan)
	return reason
}

func vecExecutable(plan logical.Plan) (*localIndexScan, string) {
	l, ok := plan.(*limit)
	if !ok {
		// Defensive: the analyzer always tops a stream plan with a limit node.
		return nil, "the plan top is not a limit node"
	}
	// A multi-group plan is dispatched by VecMergeExecutable, so reaching here means
	// one of its groups was ineligible. Report that group's reason rather than the
	// shape mismatch the single-scan walk would otherwise see.
	if mp, isMerge := l.Input.(*mergePlan); isMerge {
		for _, sp := range mp.subPlans {
			if _, reason := vecScanFrom(sp); reason != "" {
				return nil, "a group of the multi-group plan is not vec-eligible: " + reason
			}
		}
		return nil, "the multi-group plan is not vec-eligible"
	}
	return vecScanFrom(l.Input)
}

func vecScanFrom(node logical.Plan) (*localIndexScan, string) {
	scan := scanFromInput(node)
	if scan == nil {
		return nil, "the plan does not resolve to a single index scan"
	}
	if _, _, resolved := scan.vecTagProjection(); !resolved {
		return nil, "the order-by tag does not resolve against the stream schema"
	}
	return scan, ""
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
			// The vec merge reproduces the element set the row scan would hand its
			// tagFilterPlan, except for duplicate ElementIDs, whose contract is
			// filter-first — see BuildStreamMergePipeline. Whether the criteria filter
			// may run AHEAD of the merge depends on the order type — see
			// scanResumesAcrossPulls. Only an index-order scan takes the filter; a
			// timestamp-order scan leaves it at the egress, behind the cap.
			if scanResumesAcrossPulls(scan) && in.tagFilter != nil && in.tagFilter != logical.DummyFilter {
				scan.preMergeFilter, scan.filterRegistry = in.tagFilter, in.s
			}
			return scan
		}
	}
	return nil
}

// scanResumesAcrossPulls reports whether the row scan backing this plan keeps
// yielding new elements on successive Pulls, which decides whether a criteria
// (filtered) query may run its tag filter AHEAD of the vec merge.
//
// The row path nests three loops: *limit.Execute pulls tagFilterPlan.Execute until
// it has accumulated limit+offset elements, tagFilterPlan.Execute pulls the scan
// until a batch yields ≥1 match, and the scan itself caps each batch at
// maxElementSize. Whether the limit actually gets FILLED therefore depends on the
// scan resuming:
//
//   - index-order (idxResult): the sorted iterator persists across Pulls and each
//     Pull drains the next maxElementSize entries (query_by_idx.go:262), so row keeps
//     pulling and DOES fill the limit out of the whole FILTERED ordered set. Vec
//     bounds the same set by running the tag filter before the merge, so the
//     maxElementSize cap bounds the top-N of the filtered set rather than truncating
//     the input the filter has yet to see (duplicate ElementIDs excepted — see
//     BuildStreamMergePipeline).
//   - timestamp order (tsResult): one Pull consumes a whole segment and caps the
//     result at maxElementSize (query_by_ts.go:136,159); the next Pull only advances
//     to a further segment. For data inside a single segment the scan is then
//     exhausted, so row returns only the matches from that first capped batch and
//     legitimately UNDER-fills the limit (e.g. 30 scanned, 2 rejected ⇒ 28 returned).
//     Vec reproduces that under-fill only by capping BEFORE filtering, so the filter
//     is NOT pushed down here and stays at the egress.
//
// Caveat (documented, not emulated): for timestamp order spanning MULTIPLE segments
// the row scan does resume per segment, so row could accumulate past the first
// capped batch where vec stops. Reproducing that needs the scan's segment
// boundaries, which the vec merge does not see.
func scanResumesAcrossPulls(scan *localIndexScan) bool {
	return scan.order != nil && scan.order.Index != nil
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
	return nodeTagFilter(l.Input)
}

// nodeTagFilter returns the per-node tag filter, hidden-tag set, and schema when
// plan is a *tagFilterPlan (a criteria group), else ok=false. It operates on a
// plan NODE directly (not the *limit wrapper), so both the single-group VecTagFilter
// (via l.Input) and the multi-group dispatch (via each mergePlan subPlan) share the
// same extraction — the vec egress then applies the SAME per-element
// tagFilter.Match + hidden-tag strip that the row tagFilterPlan.Execute applies.
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
// (via scanFromInput, incl. orderTagProjected). If ANY subPlan is not vec-eligible,
// it returns ok=false so the whole query runs the row path — vec and row are never
// mixed across groups. The merge params (sortByTime/sortTagSpec/desc) are taken
// verbatim from the mergePlan so the cross-group order matches the row path exactly.
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

// vecTagProjection returns the tag projection the scan passes to its storage
// request, whether the ordered tag was newly added (and therefore hidden from the
// client projection), and whether it was successfully resolved against the stream
// schema.
//
// For non-index-order queries (or degenerate order shapes) it returns the client
// projection as-is. For an index-order query whose single ordered tag is already
// projected it returns the client projection as-is. When the ordered tag is
// missing from the projection, it appends it to the storage request and marks it
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
			proj.Names = append(append([]string(nil), proj.Names...), name)
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
// query the merge caps at maxElementSize like any other, because the criteria tag
// filter runs as a pre-merge fusible: the cap therefore bounds the top-N of the
// FILTERED set. The egress re-applies the same filter (it is also the hidden-tag
// strip) before the outer offset:offset+limit slice.
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
	// A criteria query runs its tag filter as a PRE-MERGE fusible, so the merge sees
	// only surviving rows and the cap keeps the top-N of the FILTERED set — not a
	// top-N taken before the filter, which the filter's unknown selectivity could
	// leave empty. The egress still applies the same filter; on rows that already
	// passed here that re-check is a no-op, and it remains the hidden-tag strip.
	var preMerge []vectorized.FusibleOperator
	if i.preMergeFilter != nil {
		preMerge = append(preMerge,
			vstream.NewTagFilter(schema, i.projectionTags, i.preMergeFilter, i.filterRegistry))
	}
	limitRows := uint32(0)
	if i.maxElementSize > 0 {
		limitRows = uint32(i.maxElementSize)
	}

	pipeline, buildErr := vstream.BuildStreamMergePipeline(
		&vecSourceOperator{source: source, schema: schema},
		schema, desc, 0, limitRows, batchSize, i.maxElementSize, preMerge...)
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
