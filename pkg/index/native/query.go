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
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	roaringpkg "github.com/RoaringBitmap/roaring"
	vellumregexp "github.com/blevesearch/vellum/regexp"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// ErrQueryLimit reports a caller-supplied term or candidate budget that was
// exceeded. Native execution never silently turns an unbounded dictionary
// expansion into an all-documents scan.
var ErrQueryLimit = errors.New("native: query budget exceeded")

// ErrInvalidQuery reports a query mode or shape that cannot be executed by
// the native primitives without changing its meaning.
var ErrInvalidQuery = errors.New("native: invalid query")

// TermSetMode controls whether a term-set selection uses union or
// intersection semantics.
type TermSetMode uint8

const (
	// MatchAnyTerm selects a document containing at least one requested term.
	MatchAnyTerm TermSetMode = iota
	// MatchAllTerms selects a document containing every requested term.
	MatchAllTerms
)

// QueryScope carries the optional series and timestamp constraints shared by
// native membership operations. SeriesID is matched against SeriesField as an
// exact encoded term. A timestamp range excludes documents without a stored
// native timestamp.
//
//nolint:govet // request fields remain grouped by the public query contract.
type QueryScope struct {
	SeriesField string
	SeriesID    []byte
	SeriesIDs   [][]byte
	TimeRange   *TimeRange
}

// TermSetRequest selects exact encoded terms from one field, ORed with every
// term the dictionary expansion of Prefix and Wildcard matches. The caller
// owns term analysis and encoding; native execution only performs byte-exact
// dictionary membership and, for Prefix/Wildcard, dictionary-bounded pattern
// matching. Field combination (Any/All, via Mode) treats Terms, each Prefix
// expansion, and each Wildcard expansion as one operand apiece. MaxTerms
// bounds the number of dictionary terms Prefix and Wildcard may expand to and
// is required when either is non-empty, like MatchRange.
//
//nolint:govet // request fields remain grouped by the public query contract.
type TermSetRequest struct {
	Field string
	Terms [][]byte
	// Prefix selects every dictionary term in the half-open byte range
	// [p, successor(p)) for each p.
	Prefix [][]byte
	// Wildcard selects every dictionary term a `*`/`?` pattern matches. `*`
	// matches zero or more bytes, `?` matches exactly one byte, and every
	// other regexp metacharacter (including `|` and `\`) is matched literally.
	Wildcard      [][]byte
	Mode          TermSetMode
	Scope         QueryScope
	MaxTerms      uint64
	MaxCandidates uint64
}

// FieldRequest selects documents having an actual indexed posting for Field.
// Stored-only values and indexed fields with no terms are absent. MaxTerms is
// required because field presence is implemented by bounded dictionary
// expansion rather than by decoding every stored document.
//
//nolint:govet // request fields remain grouped by the public query contract.
type FieldRequest struct {
	Field         string
	Scope         QueryScope
	MaxTerms      uint64
	MaxCandidates uint64
}

// RangeRequest selects terms in encoded byte order. Numeric callers provide
// the native prefix-coded endpoint bytes; no numeric library or analyzer is
// used here. A nil endpoint is unbounded, while a non-nil empty endpoint is a
// valid empty term bound.
//
//nolint:govet // request fields remain grouped by the public query contract.
type RangeRequest struct {
	Field         string
	Lower         []byte
	Upper         []byte
	IncludesLower bool
	IncludesUpper bool
	Scope         QueryScope
	MaxTerms      uint64
	MaxCandidates uint64
}

// QueryHit is an owned candidate projection. Segment and DocumentNumber are
// internal tie-break coordinates retained only while SortHits is called.
type QueryHit struct {
	Identifier     []byte
	SeriesID       []byte
	Timestamp      int64
	Segment        uint64
	DocumentNumber uint64
}

// SortRequest orders candidate-only results by the smallest encoded repeated
// doc value. Missing values sort last in either direction. Limit zero means
// return all candidates; positive limits use a bounded top-K frontier.
type SortRequest struct {
	Field  string
	Desc   bool
	Offset int
	Limit  int
}

// MatchTermsSet performs exact any/all term matching over one pinned root.
// Deletion masks and QueryScope are applied before hits escape the view.
func (v *ReadView) MatchTermsSet(ctx context.Context, request TermSetRequest) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if termSetRequestEmpty(request) {
		return nil, nil
	}
	if err := validateTermSetRequest(request); err != nil {
		return nil, err
	}
	wildcards, wildcardErr := compileWildcardAutomata(request.Wildcard)
	if wildcardErr != nil {
		return nil, wildcardErr
	}
	result := make([]QueryHit, 0)
	var totalCandidates uint64
	for segmentIndex, current := range v.root.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, ok := current.(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("match terms segment %d has type %T", segmentIndex, current)
		}
		candidates, err := exactCandidates(ctx, segment, request, wildcards)
		if err != nil {
			return nil, err
		}
		narrowed, projection, skip, err := narrowSegmentForTime(ctx, segment, candidates, request.Scope.TimeRange)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		// Counted after narrowing: a document the range excludes was never a
		// candidate the query has to consider, so it should not spend the
		// caller's limit.
		totalCandidates += narrowed.GetCardinality()
		if request.MaxCandidates != 0 && totalCandidates > request.MaxCandidates {
			return nil, ErrQueryLimit
		}
		hits, err := projectCandidates(ctx, uint64(segmentIndex), segment, narrowed, request.Scope.TimeRange, projection)
		if err != nil {
			return nil, err
		}
		result = append(result, hits...)
	}
	return result, nil
}

// termSetRequestEmpty reports a request with no literal terms and no pattern
// to expand, which every term-set entry point treats as a no-op match.
func termSetRequestEmpty(request TermSetRequest) bool {
	return request.Field == "" || (len(request.Terms) == 0 && len(request.Prefix) == 0 && len(request.Wildcard) == 0)
}

// validateTermSetRequest enforces that Prefix/Wildcard expansion, like
// MatchRange, is always bounded.
func validateTermSetRequest(request TermSetRequest) error {
	if (len(request.Prefix) != 0 || len(request.Wildcard) != 0) && request.MaxTerms == 0 {
		return fmt.Errorf("term set on %q requires MaxTerms: %w", request.Field, ErrQueryLimit)
	}
	return nil
}

// MatchAllTermSets intersects several exact term sets at posting level and
// projects only the documents every set matches, so a selective conjunct
// bounds stored-document decoding no matter how broad the others are. The
// first request's Scope.TimeRange applies to projection; later requests must
// not set one.
func (v *ReadView) MatchAllTermSets(ctx context.Context, requests []TermSetRequest) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return nil, nil
	}
	requestWildcards := make([][]nativeice.DictionaryAutomaton, len(requests))
	for index, request := range requests {
		if termSetRequestEmpty(request) {
			return nil, nil
		}
		if err := validateTermSetRequest(request); err != nil {
			return nil, err
		}
		if index > 0 && request.Scope.TimeRange != nil {
			return nil, fmt.Errorf("term set %d sets a time range only the first may set: %w", index, ErrInvalidQuery)
		}
		wildcards, wildcardErr := compileWildcardAutomata(request.Wildcard)
		if wildcardErr != nil {
			return nil, wildcardErr
		}
		requestWildcards[index] = wildcards
	}
	result := make([]QueryHit, 0)
	for segmentIndex, current := range v.root.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, ok := current.(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("match term sets segment %d has type %T", segmentIndex, current)
		}
		var candidates *roaringpkg.Bitmap
		for requestIndex, request := range requests {
			matched, err := exactCandidates(ctx, segment, request, requestWildcards[requestIndex])
			if err != nil {
				return nil, err
			}
			if candidates == nil {
				candidates = matched
			} else {
				candidates.And(matched)
			}
			if candidates.IsEmpty() {
				break
			}
		}
		narrowed, projection, skip, narrowErr := narrowSegmentForTime(ctx, segment, candidates, requests[0].Scope.TimeRange)
		if narrowErr != nil {
			return nil, narrowErr
		}
		if skip {
			continue
		}
		hits, err := projectCandidates(ctx, uint64(segmentIndex), segment, narrowed, requests[0].Scope.TimeRange, projection)
		if err != nil {
			return nil, err
		}
		result = append(result, hits...)
	}
	return result, nil
}

// FilterTermsSet keeps the hits, taken from this view, whose documents also
// match request. It tests posting membership only, so no stored document is
// decoded. request must not set Scope.TimeRange.
func (v *ReadView) FilterTermsSet(ctx context.Context, hits []QueryHit, request TermSetRequest) ([]QueryHit, error) {
	if termSetRequestEmpty(request) {
		return nil, nil
	}
	if request.Scope.TimeRange != nil {
		return nil, fmt.Errorf("filter terms on %q cannot apply a time range: %w", request.Field, ErrInvalidQuery)
	}
	if err := validateTermSetRequest(request); err != nil {
		return nil, err
	}
	wildcards, wildcardErr := compileWildcardAutomata(request.Wildcard)
	if wildcardErr != nil {
		return nil, wildcardErr
	}
	return v.filterHits(ctx, hits, request.MaxCandidates, func(segment *memorySegment) (*roaringpkg.Bitmap, error) {
		return exactCandidates(ctx, segment, request, wildcards)
	})
}

// FilterRange keeps the hits, taken from this view, whose documents also fall
// in request's range, without decoding any stored document. request must not
// set Scope.TimeRange.
func (v *ReadView) FilterRange(ctx context.Context, hits []QueryHit, request RangeRequest) ([]QueryHit, error) {
	if request.Field == "" || request.MaxTerms == 0 {
		return nil, fmt.Errorf("range on %q requires MaxTerms: %w", request.Field, ErrQueryLimit)
	}
	if request.Scope.TimeRange != nil {
		return nil, fmt.Errorf("filter range on %q cannot apply a time range: %w", request.Field, ErrInvalidQuery)
	}
	return v.filterHits(ctx, hits, request.MaxCandidates, func(segment *memorySegment) (*roaringpkg.Bitmap, error) {
		return rangeCandidates(ctx, segment, request)
	})
}

func (v *ReadView) filterHits(
	ctx context.Context, hits []QueryHit, maxCandidates uint64, candidatesOf func(*memorySegment) (*roaringpkg.Bitmap, error),
) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	bySegment := make(map[uint64]*roaringpkg.Bitmap)
	var totalCandidates uint64
	result := make([]QueryHit, 0, len(hits))
	for _, hit := range hits {
		candidates, cached := bySegment[hit.Segment]
		if !cached {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if hit.Segment >= uint64(len(v.root.segments)) {
				return nil, fmt.Errorf("hit segment %d is outside this view: %w", hit.Segment, ErrInvalidQuery)
			}
			segment, ok := v.root.segments[hit.Segment].(*memorySegment)
			if !ok {
				return nil, fmt.Errorf("filter segment %d has type %T", hit.Segment, v.root.segments[hit.Segment])
			}
			var err error
			candidates, err = candidatesOf(segment)
			if err != nil {
				return nil, err
			}
			totalCandidates += candidates.GetCardinality()
			if maxCandidates != 0 && totalCandidates > maxCandidates {
				return nil, ErrQueryLimit
			}
			bySegment[hit.Segment] = candidates
		}
		if hit.DocumentNumber <= math.MaxUint32 && candidates.Contains(uint32(hit.DocumentNumber)) {
			result = append(result, hit)
		}
	}
	return result, nil
}

// MatchField selects indexed-present documents using bounded dictionary
// expansion. It deliberately does not substitute schema or stored-field
// presence for posting membership.
func (v *ReadView) MatchField(ctx context.Context, request FieldRequest) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if request.Field == "" || request.MaxTerms == 0 {
		return nil, fmt.Errorf("field %q requires MaxTerms: %w", request.Field, ErrQueryLimit)
	}
	result := make([]QueryHit, 0)
	var totalCandidates uint64
	for segmentIndex, current := range v.root.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, ok := current.(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("match field segment %d has type %T", segmentIndex, current)
		}
		candidates, err := rangeCandidates(ctx, segment, RangeRequest{
			Field: request.Field, Scope: request.Scope, MaxTerms: request.MaxTerms, MaxCandidates: request.MaxCandidates,
		})
		if err != nil {
			return nil, err
		}
		totalCandidates += candidates.GetCardinality()
		if request.MaxCandidates != 0 && totalCandidates > request.MaxCandidates {
			return nil, ErrQueryLimit
		}
		narrowed, projection, skip, err := narrowSegmentForTime(ctx, segment, candidates, request.Scope.TimeRange)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		hits, err := projectCandidates(ctx, uint64(segmentIndex), segment, narrowed, request.Scope.TimeRange, projection)
		if err != nil {
			return nil, err
		}
		result = append(result, hits...)
	}
	return result, nil
}

// MatchRange selects encoded dictionary terms between the requested bounds.
// The dictionary remains bounded by MaxTerms and postings are combined before
// candidate-only identifier/timestamp projection.
func (v *ReadView) MatchRange(ctx context.Context, request RangeRequest) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if request.Field == "" || request.MaxTerms == 0 {
		return nil, fmt.Errorf("range on %q requires MaxTerms: %w", request.Field, ErrQueryLimit)
	}
	result := make([]QueryHit, 0)
	var totalCandidates uint64
	for segmentIndex, current := range v.root.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, ok := current.(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("match range segment %d has type %T", segmentIndex, current)
		}
		candidates, err := rangeCandidates(ctx, segment, request)
		if err != nil {
			return nil, err
		}
		narrowed, projection, skip, err := narrowSegmentForTime(ctx, segment, candidates, request.Scope.TimeRange)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		totalCandidates += narrowed.GetCardinality()
		if request.MaxCandidates != 0 && totalCandidates > request.MaxCandidates {
			return nil, ErrQueryLimit
		}
		hits, err := projectCandidates(ctx, uint64(segmentIndex), segment, narrowed, request.Scope.TimeRange, projection)
		if err != nil {
			return nil, err
		}
		result = append(result, hits...)
	}
	return result, nil
}

// SortHits orders only the supplied candidates. It reads one candidate's doc
// values at a time and retains at most Offset+Limit items when a positive
// limit is requested; ICE doc values are not assumed to be sorted streams.
func (v *ReadView) SortHits(ctx context.Context, hits []QueryHit, request SortRequest) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if request.Offset < 0 || request.Limit < 0 {
		return nil, fmt.Errorf("negative sort page: %w", ErrQueryLimit)
	}
	if request.Field == "" {
		return nil, errors.New("native: sort field is required")
	}
	if request.Limit > 0 && request.Offset > math.MaxInt-request.Limit {
		return nil, fmt.Errorf("sort page overflows: %w", ErrQueryLimit)
	}
	keep := 0
	if request.Limit > 0 {
		keep = request.Offset + request.Limit
	}
	ordered := make([]sortableHit, 0, func() int {
		if request.Limit == 0 {
			return len(hits)
		}
		return keep
	}())
	var frontier *sortFrontier
	if request.Limit > 0 {
		frontier = &sortFrontier{items: make([]sortableHit, 0, keep)}
	}
	for _, hit := range hits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, err := v.segmentAt(hit.Segment)
		if err != nil {
			return nil, err
		}
		values, err := segment.handle.reader.DocumentValues(request.Field, hit.DocumentNumber)
		if err != nil {
			return nil, err
		}
		value, missing := smallestDocValue(values)
		item := sortableHit{hit: hit, value: value, missing: missing, desc: request.Desc}
		if frontier != nil {
			if len(frontier.items) < keep {
				heap.Push(frontier, item)
			} else if item.less(frontier.items[0]) {
				heap.Pop(frontier)
				heap.Push(frontier, item)
			}
		} else {
			ordered = append(ordered, item)
		}
	}
	if request.Limit == 0 {
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].less(ordered[j]) })
	} else {
		ordered = frontier.items
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].less(ordered[j]) })
	}
	start := minInt(request.Offset, len(ordered))
	end := len(ordered)
	if request.Limit > 0 {
		end = minInt(start+request.Limit, len(ordered))
	}
	result := make([]QueryHit, end-start)
	for i := range result {
		result[i] = ordered[start+i].hit
	}
	return result, nil
}

// ProjectedHit contains one candidate's owned stored fields and coordinates.
// Segment and DocumentNumber remain available for stable physical identity.
//
//nolint:govet // embedding preserves the public candidate coordinate shape.
type ProjectedHit struct {
	QueryHit
	Fields map[string][][]byte
}

// ProjectHit reads one physical candidate from the pinned root without an
// identifier posting lookup. All returned bytes are owned by the caller.
func (v *ReadView) ProjectHit(ctx context.Context, hit QueryHit, fields ...string) (ProjectedHit, error) {
	if err := v.check(ctx); err != nil {
		return ProjectedHit{}, err
	}
	segment, err := v.segmentAt(hit.Segment)
	if err != nil {
		return ProjectedHit{}, err
	}
	if hit.DocumentNumber >= segment.handle.count {
		return ProjectedHit{}, fmt.Errorf("document %d is unavailable: %w", hit.DocumentNumber, ErrCorrupt)
	}
	if _, deleted := segment.deleted[hit.DocumentNumber]; deleted {
		return ProjectedHit{}, fmt.Errorf("document %d is deleted: %w", hit.DocumentNumber, ErrCorrupt)
	}
	wanted := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		wanted[field] = struct{}{}
	}
	result := ProjectedHit{QueryHit: QueryHit{
		Identifier: bytes.Clone(hit.Identifier), SeriesID: bytes.Clone(hit.SeriesID), Timestamp: hit.Timestamp,
		Segment: hit.Segment, DocumentNumber: hit.DocumentNumber,
	}, Fields: make(map[string][][]byte, len(fields))}
	var timestampErr error
	visitErr := segment.handle.reader.VisitDocument(hit.DocumentNumber, func(document nativeice.StoredDocument) error {
		return document.VisitStoredFields(func(name string, value []byte) bool {
			if err := ctx.Err(); err != nil {
				timestampErr = err
				return false
			}
			if name == identifierField {
				result.Identifier = bytes.Clone(value)
				return true
			}
			if name == timestampField {
				result.Timestamp, timestampErr = nativeice.DecodePrefixCodedInt64(value)
				return timestampErr == nil
			}
			if _, ok := wanted[name]; ok {
				result.Fields[name] = append(result.Fields[name], bytes.Clone(value))
			}
			return true
		})
	})
	if visitErr != nil {
		return ProjectedHit{}, visitErr
	}
	if timestampErr != nil {
		return ProjectedHit{}, timestampErr
	}
	return result, nil
}

// ProjectSortValue returns the smallest copied doc value for one candidate.
func (v *ReadView) ProjectSortValue(ctx context.Context, hit QueryHit, field string) ([]byte, bool, error) {
	if err := v.check(ctx); err != nil {
		return nil, false, err
	}
	segment, err := v.segmentAt(hit.Segment)
	if err != nil {
		return nil, false, err
	}
	if hit.DocumentNumber >= segment.handle.count {
		return nil, false, fmt.Errorf("document %d is unavailable: %w", hit.DocumentNumber, ErrCorrupt)
	}
	if _, deleted := segment.deleted[hit.DocumentNumber]; deleted {
		return nil, false, fmt.Errorf("document %d is deleted: %w", hit.DocumentNumber, ErrCorrupt)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, false, ctxErr
	}
	values, err := segment.handle.reader.DocumentValues(field, hit.DocumentNumber)
	if err != nil {
		return nil, false, err
	}
	value, missing := smallestDocValue(values)
	return bytes.Clone(value), missing, nil
}

//nolint:govet // hit coordinates and ordering metadata are intentionally grouped.
type sortableHit struct {
	hit     QueryHit
	value   []byte
	missing bool
	desc    bool
}

func (h sortableHit) less(other sortableHit) bool {
	if h.missing != other.missing {
		return !h.missing
	}
	if !h.missing && !bytes.Equal(h.value, other.value) {
		if h.desc {
			return bytes.Compare(h.value, other.value) > 0
		}
		return bytes.Compare(h.value, other.value) < 0
	}
	if !bytes.Equal(h.hit.Identifier, other.hit.Identifier) {
		return bytes.Compare(h.hit.Identifier, other.hit.Identifier) < 0
	}
	if h.hit.Segment != other.hit.Segment {
		return h.hit.Segment < other.hit.Segment
	}
	return h.hit.DocumentNumber < other.hit.DocumentNumber
}

// sortFrontier keeps the worst retained item at the root.
type sortFrontier struct{ items []sortableHit }

func (h sortFrontier) Len() int { return len(h.items) }
func (h sortFrontier) Less(i, j int) bool {
	return h.items[j].less(h.items[i])
}
func (h sortFrontier) Swap(i, j int)   { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *sortFrontier) Push(value any) { h.items = append(h.items, value.(sortableHit)) }
func (h *sortFrontier) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}

// exactCandidates resolves one term-set request to the bitmap of documents it
// selects within segment. Terms, each Prefix entry's dictionary-range
// expansion, and each Wildcard entry's automaton-filtered expansion are each
// treated as one operand, combined under Mode (Any = Or, All = And) exactly
// as a plain literal-terms request combines its operands. Prefix and Wildcard
// expansion is capped by MaxTerms, counted across every Prefix and Wildcard
// entry for this segment, mirroring MatchRange.
//
//nolint:contextcheck // nativeice exact posting decode has no context-capable variant yet.
func exactCandidates(
	ctx context.Context, segment *memorySegment, request TermSetRequest, wildcards []nativeice.DictionaryAutomaton,
) (*roaringpkg.Bitmap, error) {
	if request.Mode != MatchAnyTerm && request.Mode != MatchAllTerms {
		return nil, fmt.Errorf("term mode %d: %w", request.Mode, ErrInvalidQuery)
	}
	var result *roaringpkg.Bitmap
	combine := func(candidate *roaringpkg.Bitmap) {
		switch {
		case result == nil:
			result = candidate
		case request.Mode == MatchAllTerms:
			result.And(candidate)
		default:
			result.Or(candidate)
		}
	}
	// Once an All-mode intersection is already empty, it can never become
	// non-empty again: skip remaining operands, including dictionary
	// expansion and its budget accounting, since it cannot change the result.
	stopped := func() bool {
		return request.Mode == MatchAllTerms && result != nil && result.IsEmpty()
	}
	for _, term := range request.Terms {
		if stopped() {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		candidate, found, err := segment.handle.reader.TermPostingBitmap(request.Field, term)
		if err != nil {
			return nil, err
		}
		if !found {
			candidate = roaringpkg.New()
		}
		combine(candidate)
		if exceedsCandidates(result, request.MaxCandidates) {
			return nil, ErrQueryLimit
		}
	}
	var expanded uint64
	for _, prefix := range request.Prefix {
		if stopped() {
			break
		}
		candidate, err := dictionaryMatchCandidates(ctx, segment, request.Field, nil, prefix, successor(prefix), request.MaxTerms, &expanded)
		if err != nil {
			return nil, err
		}
		combine(candidate)
		if exceedsCandidates(result, request.MaxCandidates) {
			return nil, ErrQueryLimit
		}
	}
	for patternIndex := range request.Wildcard {
		if stopped() {
			break
		}
		var automaton nativeice.DictionaryAutomaton
		if patternIndex < len(wildcards) {
			automaton = wildcards[patternIndex]
		}
		candidate, err := dictionaryMatchCandidates(ctx, segment, request.Field, automaton, nil, nil, request.MaxTerms, &expanded)
		if err != nil {
			return nil, err
		}
		combine(candidate)
		if exceedsCandidates(result, request.MaxCandidates) {
			return nil, ErrQueryLimit
		}
	}
	if result == nil {
		result = roaringpkg.New()
	}
	return applySeries(ctx, segment, result, request.Scope, request.MaxCandidates)
}

// dictionaryMatchCandidates ORs the posting bitmaps of every dictionary term
// in [start, end) that automaton also accepts (a nil automaton accepts every
// term in range). Each matched term counts against maxTerms, shared across
// every Prefix/Wildcard entry of one exactCandidates call via expanded.
//
//nolint:contextcheck // nativeice dictionary/posting cursors are synchronously bounded.
func dictionaryMatchCandidates(
	ctx context.Context, segment *memorySegment, field string, automaton nativeice.DictionaryAutomaton,
	start, end []byte, maxTerms uint64, expanded *uint64,
) (*roaringpkg.Bitmap, error) {
	iterator, err := segment.handle.reader.NewDictionaryTermIterator(field, automaton, start, end)
	if err != nil {
		return nil, err
	}
	defer func() { _ = iterator.Close() }()
	result := roaringpkg.New()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		term, present, nextErr := iterator.NextKey()
		if nextErr != nil {
			return nil, nextErr
		}
		if !present {
			break
		}
		*expanded++
		if maxTerms != 0 && *expanded > maxTerms {
			return nil, ErrQueryLimit
		}
		posting, found, postingErr := segment.handle.reader.TermPostingBitmap(field, term)
		if postingErr != nil {
			return nil, postingErr
		}
		if found {
			result.Or(posting)
		}
	}
	return result, nil
}

// successor returns the exclusive upper bound of the half-open byte range
// sharing prefix as a common prefix, or nil when prefix has no successor
// (every byte already 0xFF), which leaves the range unbounded above.
func successor(prefix []byte) []byte {
	bound := append([]byte(nil), prefix...)
	for index := len(bound) - 1; index >= 0; index-- {
		if bound[index] != 0xFF {
			bound[index]++
			return bound[:index+1]
		}
	}
	return nil
}

// wildcardRegexpReplacer escapes regexp metacharacters (including `|` and
// `\`) and rewrites `*`/`?` to their regexp equivalents, matching the
// conversion the legacy series index relied on through the previous
// release's wildcard-query support.
var wildcardRegexpReplacer = strings.NewReplacer(
	"+", `\+`,
	"(", `\(`,
	")", `\)`,
	"^", `\^`,
	"$", `\$`,
	".", `\.`,
	"{", `\{`,
	"}", `\}`,
	"[", `\[`,
	"]", `\]`,
	`|`, `\|`,
	`\`, `\\`,
	"*", ".*",
	"?", ".",
)

// compileWildcardAutomaton converts one `*`/`?` wildcard pattern into the
// dictionary automaton that selects every term it matches.
func compileWildcardAutomaton(pattern []byte) (nativeice.DictionaryAutomaton, error) {
	escaped := wildcardRegexpReplacer.Replace(string(pattern))
	automaton, err := vellumregexp.New(escaped)
	if err != nil {
		return nil, fmt.Errorf("compile wildcard pattern %q: %w", pattern, err)
	}
	return automaton, nil
}

// compileWildcardAutomata compiles every wildcard pattern once, index-aligned
// with patterns, so a multi-segment request pays each pattern's regexp
// compilation once rather than once per segment.
func compileWildcardAutomata(patterns [][]byte) ([]nativeice.DictionaryAutomaton, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	automata := make([]nativeice.DictionaryAutomaton, len(patterns))
	for index, pattern := range patterns {
		automaton, err := compileWildcardAutomaton(pattern)
		if err != nil {
			return nil, err
		}
		automata[index] = automaton
	}
	return automata, nil
}

//nolint:contextcheck // nativeice dictionary/posting cursors are synchronously bounded.
func rangeCandidates(ctx context.Context, segment *memorySegment, request RangeRequest) (*roaringpkg.Bitmap, error) {
	iterator, err := segment.handle.reader.NewDictionaryTermIterator(request.Field, nil, request.Lower, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = iterator.Close() }()
	result := roaringpkg.New()
	var termCount uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		term, present, nextErr := iterator.NextKey()
		if nextErr != nil {
			return nil, nextErr
		}
		if !present {
			break
		}
		if request.Lower != nil && bytes.Equal(term, request.Lower) && !request.IncludesLower {
			continue
		}
		if request.Upper != nil {
			comparison := bytes.Compare(term, request.Upper)
			if comparison > 0 || comparison == 0 && !request.IncludesUpper {
				break
			}
		}
		termCount++
		if termCount > request.MaxTerms {
			return nil, ErrQueryLimit
		}
		posting, found, postingErr := segment.handle.reader.TermPostingBitmap(request.Field, term)
		if postingErr != nil {
			return nil, postingErr
		}
		if found {
			result.Or(posting)
			if exceedsCandidates(result, request.MaxCandidates) {
				return nil, ErrQueryLimit
			}
		}
	}
	return applySeries(ctx, segment, result, request.Scope, request.MaxCandidates)
}

//nolint:contextcheck // nativeice exact posting decode has no context-capable variant yet.
func applySeries(ctx context.Context, segment *memorySegment, result *roaringpkg.Bitmap, scope QueryScope, maxCandidates uint64) (*roaringpkg.Bitmap, error) {
	if scope.SeriesField == "" {
		return result, nil
	}
	series := scope.SeriesIDs
	if len(series) == 0 {
		series = [][]byte{scope.SeriesID}
	}
	seriesCandidates := roaringpkg.New()
	for _, seriesID := range series {
		posting, found, err := segment.handle.reader.TermPostingBitmap(scope.SeriesField, seriesID)
		if err != nil {
			return nil, err
		}
		if found {
			seriesCandidates.Or(posting)
		}
	}
	result.And(seriesCandidates)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if exceedsCandidates(result, maxCandidates) {
		return nil, ErrQueryLimit
	}
	return result, nil
}

// seriesCandidates returns the bounded OR of one or more exact series
// postings. It is kept separate from applySeries because an all-document
// sort selection needs the union before applying it.
//
//nolint:contextcheck // nativeice term posting access has no context-capable variant yet.
func seriesCandidates(ctx context.Context, segment *memorySegment, scope QueryScope) (*roaringpkg.Bitmap, error) {
	result := roaringpkg.New()
	if scope.SeriesField == "" {
		return result, nil
	}
	series := scope.SeriesIDs
	if len(series) == 0 {
		series = [][]byte{scope.SeriesID}
	}
	for _, seriesID := range series {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		posting, found, err := segment.handle.reader.TermPostingBitmap(scope.SeriesField, seriesID)
		if err != nil {
			return nil, err
		}
		if found {
			result.Or(posting)
		}
	}
	return result, nil
}

// timeProjection says what projection must do with a candidate's timestamp.
type timeProjection uint8

const (
	// timeProjectionCheck compares each decoded timestamp against the range,
	// which is what a segment the range only partly covers, or one that cannot
	// use the trie, still needs.
	timeProjectionCheck timeProjection = iota
	// timeProjectionContained skips the range comparison because every
	// timestamped document in the segment is already known to be in range,
	// either because the range contains the segment or because the trie
	// selected exactly the in-range documents. Documents without a timestamp
	// are still dropped, which the hasTimestamp check keeps doing.
	timeProjectionContained
)

// narrowSegmentForTime classifies the segment against the query range and
// returns the candidates projection should walk.
//
// skip is true when the segment cannot contribute any hit, so the caller can
// move on without decoding anything and without counting those candidates
// against its limit. The returned candidate set is already narrowed, so a
// limit check after this point no longer counts documents the range excludes.
func narrowSegmentForTime(
	ctx context.Context,
	segment *memorySegment,
	candidates *roaringpkg.Bitmap,
	timeRange *TimeRange,
) (narrowed *roaringpkg.Bitmap, projection timeProjection, skip bool, err error) {
	class := classifyTime(segment.handle, timeRange)
	if class == timeDisjoint {
		return nil, timeProjectionContained, true, nil
	}
	narrowed, exact, narrowErr := narrowCandidatesToRange(ctx, segment, candidates, class, timeRange)
	if narrowErr != nil {
		return nil, timeProjectionContained, false, narrowErr
	}
	projection = timeProjectionCheck
	if exact {
		projection = timeProjectionContained
	}
	return narrowed, projection, false, nil
}

func projectCandidates(
	ctx context.Context,
	segmentIndex uint64,
	segment *memorySegment,
	candidates *roaringpkg.Bitmap,
	timeRange *TimeRange,
	projection timeProjection,
) ([]QueryHit, error) {
	if candidates == nil || candidates.IsEmpty() {
		return nil, nil
	}
	result := make([]QueryHit, 0, candidates.GetCardinality())
	iterator := candidates.Iterator()
	for iterator.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		documentNumber := uint64(iterator.Next())
		if _, deleted := segment.deleted[documentNumber]; deleted {
			continue
		}
		var identifier []byte
		var seriesID []byte
		var timestamp int64
		var hasTimestamp bool
		var timestampErr error
		visitErr := segment.handle.reader.VisitDocument(documentNumber, func(document nativeice.StoredDocument) error {
			return document.VisitStoredFields(func(name string, value []byte) bool {
				switch name {
				case identifierField:
					identifier = append([]byte(nil), value...)
				case seriesIDField:
					seriesID = append([]byte(nil), value...)
				case timestampField:
					timestamp, timestampErr = nativeice.DecodePrefixCodedInt64(value)
					if timestampErr == nil {
						hasTimestamp = true
					}
				}
				return timestampErr == nil
			})
		})
		if visitErr != nil {
			return nil, visitErr
		}
		if timestampErr != nil {
			return nil, fmt.Errorf("decode timestamp: %w", timestampErr)
		}
		if identifier == nil {
			return nil, fmt.Errorf("document %d has no identifier: %w", documentNumber, ErrCorrupt)
		}
		// A time range is the only thing that requires a document to carry a
		// timestamp: with no range, an untimestamped document is a normal
		// result. Where there is a range, the document must have one, and the
		// range comparison itself is only needed when the candidate set is not
		// already known to be in range.
		if timeRange != nil {
			if !hasTimestamp {
				continue
			}
			if projection == timeProjectionCheck && !timeRange.contains(timestamp) {
				continue
			}
		}
		result = append(result, QueryHit{Identifier: identifier, SeriesID: seriesID, Timestamp: timestamp, Segment: segmentIndex, DocumentNumber: documentNumber})
	}
	return result, nil
}

func exceedsCandidates(candidates *roaringpkg.Bitmap, limit uint64) bool {
	return limit != 0 && candidates.GetCardinality() > limit
}

func (v *ReadView) segmentAt(index uint64) (*memorySegment, error) {
	if v == nil || v.root == nil || index >= uint64(len(v.root.segments)) {
		return nil, fmt.Errorf("native: segment %d is unavailable", index)
	}
	segment, ok := v.root.segments[index].(*memorySegment)
	if !ok {
		return nil, fmt.Errorf("native: segment %d has type %T", index, v.root.segments[index])
	}
	return segment, nil
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func smallestDocValue(values [][]byte) ([]byte, bool) {
	if len(values) == 0 {
		return nil, true
	}
	value := bytes.Clone(values[0])
	for _, candidate := range values[1:] {
		if bytes.Compare(candidate, value) < 0 {
			value = bytes.Clone(candidate)
		}
	}
	return value, false
}
