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

package native

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// LadderFieldSuffix is appended to the name of a sort field to name its companion ladder field. The companion holds, for
// every document that carries a scalar ladder key, the prefix-coded terms of that key at each precision level (shift 0 to
// 60 in steps of 4), the same numeric trie the engine keeps for _timestamp. It is a separate field because a sort field
// may be an opaque keyword whose dictionary range queries must not see prefix-coded terms.
const LadderFieldSuffix = "#t"

// ladderEncodeField builds the companion ladder field of a Ladder field.
func ladderEncodeField(field Field) nativeice.EncodeField {
	terms := make([]nativeice.EncodeTerm, 0, trieTopShift/trieLevelStep+1)
	for shift := uint(0); shift <= trieTopShift; shift += trieLevelStep {
		terms = append(terms, nativeice.EncodeTerm{Value: nativeice.EncodePrefixCodedInt64Shift(field.LadderKey, shift), Frequency: 1})
	}
	return nativeice.EncodeField{Name: field.Name + LadderFieldSuffix, Value: nil, Terms: terms, Index: true}
}

// ladderDescents counts the trie descents TopFieldHits and SortCursor run on a field ladder. Tests read it to prove that the
// ladder, not the exhaustive fallback, served a request.
var ladderDescents atomic.Int64

// ladderFallbacks counts the descents whose result could not be trusted and was replaced by ranking every candidate. Tests
// read it to prove that a request was served by the descent itself.
var ladderFallbacks atomic.Int64

// ladderComplete reports that every document of the segment that carries the sort field is also on the field's ladder, the
// only state in which a descent may stand in for ranking every candidate. A segment written without the ladder, or merged
// from segments of which some were, has fewer ladder documents than field documents and reports false.
func (h *segmentHandle) ladderComplete(field string) bool {
	ladderDocuments, _, err := h.reader.FieldStats(field + LadderFieldSuffix)
	if err != nil || ladderDocuments == 0 {
		return false
	}
	fieldDocuments, _, err := h.reader.FieldStats(field)
	return err == nil && ladderDocuments == fieldDocuments
}

// ladderSelection returns the documents of live that can rank among the first limit by field, or false when the ladder
// cannot stand in for ranking all of live: no complete ladder, few candidates, or documents without the field that would
// have to fill the page. The returned set holds no deleted document.
func ladderSelection(
	ctx context.Context, segment *memorySegment, field string, live *roaringpkg.Bitmap, desc bool, limit uint64,
) (*roaringpkg.Bitmap, bool, error) {
	if live == nil || live.GetCardinality() <= limit*topTimeKeepFactor || !segment.handle.ladderComplete(field) {
		return nil, false, nil
	}
	if len(segment.deleted) > 0 {
		live = live.Clone()
		for number := range segment.deleted {
			live.Remove(uint32(number))
		}
		if live.GetCardinality() <= limit*topTimeKeepFactor {
			return nil, false, nil
		}
	}
	ladderDescents.Add(1)
	selected, err := trieDescent(ctx, segment, field+LadderFieldSuffix, live, 0, math.MaxUint64, desc, limit)
	if err != nil {
		return nil, false, err
	}
	if selected.GetCardinality() < limit {
		ladderFallbacks.Add(1)
		return nil, false, nil
	}
	return selected, true, nil
}

// TopFieldRequest asks for the first Limit documents, ordered by the sort doc value of Field, among the candidates of one
// term-set request. Candidates.Scope.TimeRange is optional.
//
//nolint:govet // request fields are grouped by meaning, like the other native requests.
type TopFieldRequest struct {
	Candidates TermSetRequest
	Field      string
	Limit      int
	Desc       bool
}

// topFieldCandidateBuilds counts the per-segment candidate sets a TopFieldPlan has built. Tests read it (through
// TopFieldCandidateBuilds) to prove that a search that ranks in several chunks builds each segment's candidates once.
var topFieldCandidateBuilds atomic.Int64

// TopFieldCandidateBuilds returns how many per-segment candidate sets TopFieldHits and TopFieldPlan have built so far.
// It exists for tests only.
func TopFieldCandidateBuilds() int64 { return topFieldCandidateBuilds.Load() }

// topPlanSegment is one segment of a TopFieldPlan: its candidates, already narrowed to the time range.
type topPlanSegment struct {
	segment    *memorySegment
	narrowed   *roaringpkg.Bitmap
	index      uint64
	projection timeProjection
}

// TopFieldPlan holds the candidates of a TopFieldRequest, narrowed per segment, so that the first limit documents can be
// asked for several times (a search that grows its chunk until enough survive a filter) without building the candidates
// again. The plan reads the view's segments: it must not be used after the view it came from is closed, and its bitmaps
// are never modified after they are built.
type TopFieldPlan struct {
	segments []topPlanSegment
	view     *ReadView
	request  TopFieldRequest
}

// NewTopFieldPlan builds the candidates of request in every segment once. request.Limit is ignored; Top takes the limit.
func (v *ReadView) NewTopFieldPlan(ctx context.Context, request TopFieldRequest) (*TopFieldPlan, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if request.Field == "" {
		return nil, fmt.Errorf("top field requires a field: %w", ErrInvalidQuery)
	}
	plan := &TopFieldPlan{view: v, request: request}
	if termSetRequestEmpty(request.Candidates) {
		return plan, nil
	}
	if err := validateTermSetRequest(request.Candidates); err != nil {
		return nil, err
	}
	patterns, patternErr := compileTermPatterns(ctx, request.Candidates, nil)
	if patternErr != nil {
		return nil, patternErr
	}
	timeRange := request.Candidates.Scope.TimeRange
	for segmentIndex := range v.root.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, err := v.segmentAt(uint64(segmentIndex))
		if err != nil {
			return nil, err
		}
		topFieldCandidateBuilds.Add(1)
		candidates, err := exactCandidates(ctx, segment, request.Candidates, patterns)
		if err != nil {
			return nil, err
		}
		narrowed, projection, skip, err := narrowSegmentForTime(ctx, v.owner, segment, candidates, timeRange)
		if err != nil {
			return nil, err
		}
		if skip || narrowed == nil || narrowed.IsEmpty() {
			continue
		}
		plan.segments = append(plan.segments, topPlanSegment{segment: segment, narrowed: narrowed, index: uint64(segmentIndex), projection: projection})
	}
	return plan, nil
}

// Top returns at most limit candidates in the order SortHits gives the plan's field, exactly as TopFieldHits does.
func (p *TopFieldPlan) Top(ctx context.Context, limit int) ([]QueryHit, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("top field requires a positive limit: %w", ErrInvalidQuery)
	}
	request := p.request
	request.Limit = limit
	var best []sortableHit
	for i := range p.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry := &p.segments[i]
		ranked, err := rankSegmentTop(ctx, entry.segment, entry.index, entry.narrowed, entry.projection, request)
		if err != nil {
			return nil, err
		}
		best = mergeTopField(best, ranked, limit)
	}
	result := make([]QueryHit, len(best))
	for i := range best {
		result[i] = best[i].hit
	}
	return result, nil
}

// Candidates returns how many documents the plan ranks among, over all segments.
func (p *TopFieldPlan) Candidates() uint64 {
	var total uint64
	for i := range p.segments {
		total += p.segments[i].narrowed.GetCardinality()
	}
	return total
}

// Hits returns every candidate of the plan, in segment then document order, without ranking them: the universe a caller
// ranks itself when the plan's chunks would read most of it anyway.
func (p *TopFieldPlan) Hits(ctx context.Context) ([]QueryHit, error) {
	var all []QueryHit
	timeRange := p.request.Candidates.Scope.TimeRange
	for i := range p.segments {
		entry := &p.segments[i]
		hits, err := projectCandidates(ctx, entry.index, entry.segment, entry.narrowed, timeRange, entry.projection)
		if err != nil {
			return nil, err
		}
		all = append(all, hits...)
	}
	return all, nil
}

// TopFieldHits returns at most Limit candidates in the order SortHits gives Field: the smallest doc value first (the largest
// with Desc), a document without the field last in both directions, ties by identifier, segment and document. A segment
// whose ladder for Field is complete is not read in full: the descent selects the buckets that can rank inside the first
// Limit, and only those documents are decoded and ranked. Any other segment, and any segment whose descent cannot prove
// the result, ranks all its candidates, so the result never depends on whether a ladder exists.
func (v *ReadView) TopFieldHits(ctx context.Context, request TopFieldRequest) ([]QueryHit, error) {
	if request.Limit <= 0 {
		if err := v.check(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("top field requires a positive limit: %w", ErrInvalidQuery)
	}
	plan, err := v.NewTopFieldPlan(ctx, request)
	if err != nil {
		return nil, err
	}
	return plan.Top(ctx, request.Limit)
}

// rankSegmentTop ranks one segment's narrowed candidates, through the ladder descent when it can prove the result.
func rankSegmentTop(
	ctx context.Context, segment *memorySegment, segmentIndex uint64, narrowed *roaringpkg.Bitmap, projection timeProjection,
	request TopFieldRequest,
) ([]sortableHit, error) {
	timeRange := request.Candidates.Scope.TimeRange
	if projection == timeProjectionContained {
		selected, ok, err := ladderSelection(ctx, segment, request.Field, narrowed, request.Desc, uint64(request.Limit))
		if err != nil {
			return nil, err
		}
		if ok {
			hits, projectErr := projectCandidates(ctx, segmentIndex, segment, selected, timeRange, projection)
			if projectErr != nil {
				return nil, projectErr
			}
			// A selected document that the projection dropped (it has no timestamp under a time range) may have
			// displaced a document that belongs to the page, so only a complete projection is trusted.
			if uint64(len(hits)) == selected.GetCardinality() {
				return rankHits(ctx, segment, hits, request)
			}
			ladderFallbacks.Add(1)
		}
	}
	hits, err := projectCandidates(ctx, segmentIndex, segment, narrowed, timeRange, projection)
	if err != nil {
		return nil, err
	}
	return rankHits(ctx, segment, hits, request)
}

// rankHits reads the sort doc value of each hit and keeps the first Limit in order.
func rankHits(ctx context.Context, segment *memorySegment, hits []QueryHit, request TopFieldRequest) ([]sortableHit, error) {
	if len(hits) == 0 {
		return nil, nil
	}
	if err := query.ReserveAllocation(ctx, uint64(len(hits))*128); err != nil {
		return nil, err
	}
	ranked := make([]sortableHit, 0, len(hits))
	for _, hit := range hits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		values, err := segment.handle.reader.DocumentValuesContext(ctx, request.Field, hit.DocumentNumber)
		if err != nil {
			return nil, err
		}
		if err := reserveSortValueCopies(ctx, values); err != nil {
			return nil, err
		}
		value, missing := smallestDocValue(values)
		ranked = append(ranked, sortableHit{hit: hit, value: value, missing: missing, desc: request.Desc})
	}
	return mergeTopField(nil, ranked, request.Limit), nil
}

// mergeTopField folds hits into best, both in rank order, and keeps the first limit.
func mergeTopField(best, hits []sortableHit, limit int) []sortableHit {
	if len(hits) == 0 {
		return best
	}
	best = append(best, hits...)
	sort.SliceStable(best, func(i, j int) bool { return best[i].less(best[j]) })
	if len(best) > limit {
		best = best[:limit]
	}
	return best
}
