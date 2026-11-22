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
	"context"
	"fmt"
	"math"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

type postingFilterKind uint8

const (
	filterTerms postingFilterKind = iota
	filterRange
	filterAnd
	filterOr
	filterNot
	filterAll
	filterNone
)

// PostingFilter is a boolean tree over term sets and ranges that a search evaluates on the postings of one segment, before
// it decodes any document. Every leaf is evaluated against the whole segment and then combined, so a selective filter costs
// the postings it touches and not a document per candidate. A negation subtracts from the candidates the filter is applied
// to, the way a hit-level subtraction of the same condition would.
type PostingFilter struct {
	terms    TermSetRequest
	rng      RangeRequest
	children []*PostingFilter
	kind     postingFilterKind
}

// FilterTerms matches the documents a term set selects, as FilterTermsSet does. A request with no terms and no pattern
// matches nothing. The request carries no time range.
func FilterTerms(request TermSetRequest) *PostingFilter {
	return &PostingFilter{kind: filterTerms, terms: request}
}

// FilterRanges matches the documents whose field falls in a range, as FilterRange does.
func FilterRanges(request RangeRequest) *PostingFilter {
	return &PostingFilter{kind: filterRange, rng: request}
}

// AllOf matches the documents every child matches. No child matches everything.
func AllOf(children ...*PostingFilter) *PostingFilter {
	return &PostingFilter{kind: filterAnd, children: children}
}

// AnyOf matches the documents at least one child matches. No child matches nothing.
func AnyOf(children ...*PostingFilter) *PostingFilter {
	return &PostingFilter{kind: filterOr, children: children}
}

// NotOf matches the candidates the child does not match.
func NotOf(child *PostingFilter) *PostingFilter {
	return &PostingFilter{kind: filterNot, children: []*PostingFilter{child}}
}

// MatchAll matches every candidate.
func MatchAll() *PostingFilter { return &PostingFilter{kind: filterAll} }

// MatchNone matches no candidate.
func MatchNone() *PostingFilter { return &PostingFilter{kind: filterNone} }

// compiledPostingFilter is a PostingFilter whose wildcard patterns are compiled once for every segment.
type compiledPostingFilter struct {
	patterns compiledTermPatterns
	filter   *PostingFilter
	children []*compiledPostingFilter
}

// compilePostingFilter validates filter and compiles its patterns. A nil filter compiles to nil, which matches everything.
func compilePostingFilter(ctx context.Context, filter *PostingFilter) (*compiledPostingFilter, error) {
	if filter == nil {
		return nil, nil
	}
	compiled := &compiledPostingFilter{filter: filter}
	switch filter.kind {
	case filterTerms:
		if filter.terms.Scope.TimeRange != nil {
			return nil, fmt.Errorf("filter terms on %q cannot apply a time range: %w", filter.terms.Field, ErrInvalidQuery)
		}
		if termSetRequestEmpty(filter.terms) {
			return compiled, nil
		}
		if err := validateTermSetRequest(filter.terms); err != nil {
			return nil, err
		}
		patterns, err := compileTermPatterns(ctx, filter.terms, nil)
		if err != nil {
			return nil, err
		}
		compiled.patterns = patterns
	case filterRange:
		if filter.rng.Field == "" || filter.rng.MaxTerms == 0 {
			return nil, fmt.Errorf("range on %q requires MaxTerms: %w", filter.rng.Field, ErrQueryLimit)
		}
		if filter.rng.Scope.TimeRange != nil {
			return nil, fmt.Errorf("filter range on %q cannot apply a time range: %w", filter.rng.Field, ErrInvalidQuery)
		}
	case filterAnd, filterOr, filterNot:
		children := filter.children
		if filter.kind == filterAnd {
			children = mergeRangeChildren(children)
		}
		compiled.children = make([]*compiledPostingFilter, 0, len(children))
		for _, child := range children {
			compiledChild, err := compilePostingFilter(ctx, child)
			if err != nil {
				return nil, err
			}
			compiled.children = append(compiled.children, compiledChild)
		}
	case filterAll, filterNone:
	default:
		return nil, fmt.Errorf("posting filter kind %d: %w", filter.kind, ErrInvalidQuery)
	}
	return compiled, nil
}

// candidates returns the documents of segment that match the filter among universe. It never modifies universe or a
// bitmap it returns for a child: every combination builds a new bitmap.
func (c *compiledPostingFilter) candidates(ctx context.Context, segment *memorySegment, universe *roaringpkg.Bitmap) (*roaringpkg.Bitmap, error) {
	if c == nil {
		return universe, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch c.filter.kind {
	case filterAll:
		return universe, nil
	case filterNone:
		return roaringpkg.New(), nil
	case filterTerms:
		if termSetRequestEmpty(c.filter.terms) {
			return roaringpkg.New(), nil
		}
		return exactCandidates(ctx, segment, c.filter.terms, c.patterns)
	case filterRange:
		return rangeCandidates(ctx, segment, c.filter.rng)
	case filterNot:
		if len(c.children) != 1 {
			return nil, fmt.Errorf("a negation takes one child, got %d: %w", len(c.children), ErrInvalidQuery)
		}
		excluded, err := c.children[0].candidates(ctx, segment, universe)
		if err != nil {
			return nil, err
		}
		return roaringpkg.AndNot(universe, excluded), nil
	case filterAnd:
		result := universe
		for _, child := range c.children {
			if result.IsEmpty() {
				break
			}
			matched, err := child.candidates(ctx, segment, universe)
			if err != nil {
				return nil, err
			}
			result = roaringpkg.And(result, matched)
		}
		return result, nil
	case filterOr:
		result := roaringpkg.New()
		for _, child := range c.children {
			matched, err := child.candidates(ctx, segment, universe)
			if err != nil {
				return nil, err
			}
			result = roaringpkg.Or(result, matched)
		}
		return result, nil
	}
	return nil, fmt.Errorf("posting filter kind %d: %w", c.filter.kind, ErrInvalidQuery)
}

// narrowByFilter intersects the candidates of one segment with the filter, at posting level and without decoding a
// document. The candidates are never modified.
func narrowByFilter(
	ctx context.Context, segment *memorySegment, candidates *roaringpkg.Bitmap, filter *compiledPostingFilter,
) (*roaringpkg.Bitmap, error) {
	if filter == nil || candidates == nil || candidates.IsEmpty() {
		return candidates, nil
	}
	matched, err := filter.candidates(ctx, segment, candidates)
	if err != nil {
		return nil, err
	}
	return roaringpkg.And(candidates, matched), nil
}

// FilterByPosting keeps the hits, taken from this view, that the filter matches among the hits of their segment, without
// decoding any stored document. It is the hit-level counterpart of what the search methods do with a filter at posting
// level, and gives the same answer as narrowing the hits with FilterTermsSet and FilterRange one condition at a time.
func (v *ReadView) FilterByPosting(ctx context.Context, hits []QueryHit, filter *PostingFilter) ([]QueryHit, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	compiled, err := compilePostingFilter(ctx, filter)
	if err != nil {
		return nil, err
	}
	if compiled == nil {
		return hits, nil
	}
	universes := make(map[uint64]*roaringpkg.Bitmap)
	for _, hit := range hits {
		if hit.DocumentNumber > math.MaxUint32 {
			continue
		}
		universe, ok := universes[hit.Segment]
		if !ok {
			universe = roaringpkg.New()
			universes[hit.Segment] = universe
		}
		universe.Add(uint32(hit.DocumentNumber))
	}
	matches := make(map[uint64]*roaringpkg.Bitmap, len(universes))
	for segmentIndex, universe := range universes {
		if segmentIndex >= uint64(len(v.root.segments)) {
			return nil, fmt.Errorf("hit segment %d is outside this view: %w", segmentIndex, ErrInvalidQuery)
		}
		segment, ok := v.root.segments[segmentIndex].(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("filter segment %d has type %T", segmentIndex, v.root.segments[segmentIndex])
		}
		narrowed, narrowErr := narrowByFilter(ctx, segment, universe, compiled)
		if narrowErr != nil {
			return nil, narrowErr
		}
		matches[segmentIndex] = narrowed
	}
	result := make([]QueryHit, 0, len(hits))
	for _, hit := range hits {
		if matched := matches[hit.Segment]; matched != nil && hit.DocumentNumber <= math.MaxUint32 && matched.Contains(uint32(hit.DocumentNumber)) {
			result = append(result, hit)
		}
	}
	return result, nil
}

// mergeRangeChildren folds the range children of a conjunction that constrain the same field into one range with the
// tightest bounds, in the position of the first. A range expands the dictionary terms it spans, so "a <= x AND x <= b"
// as two open-ended ranges would expand both halves of the dictionary, where the one range between a and b expands only
// the terms in it. The other children keep their order.
func mergeRangeChildren(children []*PostingFilter) []*PostingFilter {
	merged := make(map[string]int, len(children))
	result := make([]*PostingFilter, 0, len(children))
	for _, child := range children {
		if child == nil || child.kind != filterRange || child.rng.Scope.TimeRange != nil {
			result = append(result, child)
			continue
		}
		at, seen := merged[child.rng.Field]
		if !seen {
			merged[child.rng.Field] = len(result)
			result = append(result, child)
			continue
		}
		result[at] = &PostingFilter{kind: filterRange, rng: intersectRanges(result[at].rng, child.rng)}
	}
	return result
}

// intersectRanges is the range both ranges of one field hold: the larger lower bound, the smaller upper bound, and the
// smaller term budget. A nil bound is unbounded.
func intersectRanges(left, right RangeRequest) RangeRequest {
	merged := left
	switch {
	case right.Lower == nil:
	case left.Lower == nil:
		merged.Lower, merged.IncludesLower = right.Lower, right.IncludesLower
	default:
		switch comparison := bytes.Compare(left.Lower, right.Lower); {
		case comparison < 0:
			merged.Lower, merged.IncludesLower = right.Lower, right.IncludesLower
		case comparison == 0:
			merged.IncludesLower = left.IncludesLower && right.IncludesLower
		}
	}
	switch {
	case right.Upper == nil:
	case left.Upper == nil:
		merged.Upper, merged.IncludesUpper = right.Upper, right.IncludesUpper
	default:
		switch comparison := bytes.Compare(left.Upper, right.Upper); {
		case comparison > 0:
			merged.Upper, merged.IncludesUpper = right.Upper, right.IncludesUpper
		case comparison == 0:
			merged.IncludesUpper = left.IncludesUpper && right.IncludesUpper
		}
	}
	if right.MaxTerms != 0 && (left.MaxTerms == 0 || right.MaxTerms < left.MaxTerms) {
		merged.MaxTerms = right.MaxTerms
	}
	return merged
}
