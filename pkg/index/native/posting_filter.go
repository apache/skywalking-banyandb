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
	"sync"

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
//
// A leaf keeps the bitmap it evaluated for each segment, so a search that evaluates the filter again (a chunk that grows, a
// fallback) expands a wildcard pattern or a range once, not once per evaluation. That makes a filter valid for the
// segments of one view only: build one filter per search and do not use it with another view.
type PostingFilter struct {
	memo     map[*memorySegment]*roaringpkg.Bitmap
	patterns compiledTermPatterns // the compiled wildcard patterns of a terms leaf, once compiled
	terms    TermSetRequest
	rng      RangeRequest
	children []*PostingFilter
	mu       sync.Mutex
	kind     postingFilterKind
	// singleTerm marks a range over a field that holds exactly one term per document, the only kind of range that may be
	// folded with another range of the same field.
	singleTerm bool
	// patternsCompiled records that patterns holds the compiled form of terms.
	patternsCompiled bool
}

// FilterTerms matches the documents a term set selects, as FilterTermsSet does. A request with no terms and no pattern
// matches nothing. The request carries no time range.
func FilterTerms(request TermSetRequest) *PostingFilter {
	return &PostingFilter{kind: filterTerms, terms: request}
}

// FilterRanges matches the documents whose field falls in a range, as FilterRange does: a document matches when any of its
// terms falls in the range.
func FilterRanges(request RangeRequest) *PostingFilter {
	return &PostingFilter{kind: filterRange, rng: request}
}

// FilterSingleTermRanges is FilterRanges over a field that holds exactly one term per document. Two such ranges of one field
// inside an AllOf fold into one range with the tightest bounds, so only the dictionary terms between the bounds are
// expanded. A field that can hold several terms (an analyzed text, an array) must not use it: a document with the terms
// "apple" and "zebra" matches "x >= m" and "x <= c" taken one at a time, and matches no term of the folded range [m, c].
func FilterSingleTermRanges(request RangeRequest) *PostingFilter {
	return &PostingFilter{kind: filterRange, rng: request, singleTerm: true}
}

// AllOf matches the documents every child matches. No child matches everything. A nested AllOf is spliced into this one,
// and the single-term ranges of one field are folded into one range, so the order the conditions were joined in does not
// decide whether two bounds of a field meet.
func AllOf(children ...*PostingFilter) *PostingFilter {
	return &PostingFilter{kind: filterAnd, children: foldRanges(flattenAnd(children))}
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
		patterns, err := filter.compiledPatterns(ctx)
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
		compiled.children = make([]*compiledPostingFilter, 0, len(filter.children))
		for _, child := range filter.children {
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
		return c.filter.leaf(segment, func() (*roaringpkg.Bitmap, error) {
			return exactCandidates(ctx, segment, c.filter.terms, c.patterns)
		})
	case filterRange:
		return c.filter.leaf(segment, func() (*roaringpkg.Bitmap, error) {
			return rangeCandidates(ctx, segment, c.filter.rng)
		})
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

// compiledPatterns compiles the wildcard patterns of a terms leaf the first time only: compiling spends the query's pattern
// budget, and a filter is evaluated once per chunk of a growing search.
func (f *PostingFilter) compiledPatterns(ctx context.Context) (compiledTermPatterns, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.patternsCompiled {
		return f.patterns, nil
	}
	patterns, err := compileTermPatterns(ctx, f.terms, nil)
	if err != nil {
		return compiledTermPatterns{}, err
	}
	f.patterns, f.patternsCompiled = patterns, true
	return patterns, nil
}

// leaf returns the bitmap a leaf matches in one segment, evaluating it the first time only.
func (f *PostingFilter) leaf(segment *memorySegment, evaluate func() (*roaringpkg.Bitmap, error)) (*roaringpkg.Bitmap, error) {
	f.mu.Lock()
	matched, evaluated := f.memo[segment]
	f.mu.Unlock()
	if evaluated {
		return matched, nil
	}
	matched, err := evaluate()
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.memo == nil {
		f.memo = make(map[*memorySegment]*roaringpkg.Bitmap)
	}
	f.memo[segment] = matched
	f.mu.Unlock()
	return matched, nil
}

// flattenAnd splices the children of nested conjunctions into one list, in order.
func flattenAnd(children []*PostingFilter) []*PostingFilter {
	flat := make([]*PostingFilter, 0, len(children))
	for _, child := range children {
		if child != nil && child.kind == filterAnd {
			flat = append(flat, flattenAnd(child.children)...)
			continue
		}
		flat = append(flat, child)
	}
	return flat
}

// foldRanges folds the single-term range children of a conjunction that constrain the same field into one range with the
// tightest bounds, in the position of the first. A range expands the dictionary terms it spans, so "a <= x AND x <= b" as
// two open-ended ranges would expand both halves of the dictionary, where the one range between a and b expands only the
// terms in it. A range over a field that may hold several terms is left alone, as are the other children, in order.
func foldRanges(children []*PostingFilter) []*PostingFilter {
	merged := make(map[string]int, len(children))
	result := make([]*PostingFilter, 0, len(children))
	for _, child := range children {
		if child == nil || child.kind != filterRange || !child.singleTerm || child.rng.Scope.TimeRange != nil {
			result = append(result, child)
			continue
		}
		at, seen := merged[child.rng.Field]
		if !seen {
			merged[child.rng.Field] = len(result)
			result = append(result, child)
			continue
		}
		result[at] = FilterSingleTermRanges(intersectRanges(result[at].rng, child.rng))
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
