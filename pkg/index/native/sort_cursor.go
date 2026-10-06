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
	"sort"

	roaringpkg "github.com/RoaringBitmap/roaring"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

// SortKey is a complete keyset cursor position. Segment and document
// coordinates make equal repeated values and duplicate identifiers stable.
// Callers should treat the byte slices as immutable and owned by the cursor.
//
//nolint:govet // key fields are grouped for the public cursor contract.
type SortKey struct {
	Value          []byte
	Missing        bool
	Identifier     []byte
	Segment        uint64
	DocumentNumber uint64
}

// SortCursorRequest describes one pinned-view candidate selection and one
// bounded ordered page. Selection terms are exact encoded bytes; analyzer and
// numeric conversion remain caller responsibilities.
type SortCursorRequest struct {
	Selection TermSetRequest
	Range     *RangeRequest
	SortField string
	Desc      bool
	PageSize  int
}

// SortedHit is a candidate-only projection with the selected sort value.
// Repeated values use the smallest encoded value and missing values sort last
// in both directions.
//
//nolint:govet // projection fields are grouped for the public cursor contract.
type SortedHit struct {
	QueryHit
	SortValue []byte
	Missing   bool
}

//nolint:govet // segment state is grouped by immutable view ownership.
type sortCursorSegment struct {
	segment    *memorySegment
	segmentID  uint64
	candidates *roaringpkg.Bitmap
	all        bool
}

// SortCursor retains per-segment candidate bitmaps and one page-sized frontier
// per call. It never materializes the complete hit list. The caller keeps the
// ReadView pinned until Close and must not use the view concurrently with its
// own Close.
//
//nolint:govet // cursor state is grouped by pinned view and continuation.
type SortCursor struct {
	view      *ReadView
	ctx       context.Context
	sortField string
	desc      bool
	pageSize  int
	timeRange *TimeRange
	after     *SortKey
	segments  []sortCursorSegment
	closed    bool
}

// NewSortCursor creates a keyset-paginated ordered cursor over one pinned
// root. A positive MaxCandidates is enforced globally across segments.
//
//nolint:contextcheck // nativeice posting access has no context-capable variant yet.
func (v *ReadView) NewSortCursor(ctx context.Context, request SortCursorRequest) (*SortCursor, error) {
	if err := v.check(ctx); err != nil {
		return nil, err
	}
	if request.SortField == "" || request.PageSize <= 0 {
		return nil, fmt.Errorf("sort cursor requires field and positive page size: %w", ErrQueryLimit)
	}
	cursor := &SortCursor{
		view: v, ctx: ctx, sortField: request.SortField, desc: request.Desc,
		pageSize: request.PageSize, timeRange: request.Selection.Scope.TimeRange,
	}
	if len(v.root.segments) == 0 {
		return cursor, nil
	}
	var total uint64
	for index, current := range v.root.segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		segment, ok := current.(*memorySegment)
		if !ok {
			return nil, fmt.Errorf("sort cursor segment %d has type %T", index, current)
		}
		var candidates *roaringpkg.Bitmap
		var candidateErr error
		all := false
		switch {
		case request.Range != nil:
			if len(request.Selection.Terms) != 0 || request.Selection.Field != "" {
				return nil, fmt.Errorf("sort cursor cannot combine range and exact selection: %w", ErrInvalidQuery)
			}
			rangeRequest := *request.Range
			if rangeRequest.Scope.SeriesField == "" {
				rangeRequest.Scope = request.Selection.Scope
			}
			candidates, candidateErr = rangeCandidates(ctx, segment, rangeRequest)
			if candidateErr != nil {
				return nil, candidateErr
			}
			total += candidates.GetCardinality()
		case len(request.Selection.Terms) == 0:
			if request.Selection.Field != "" {
				return nil, fmt.Errorf("sort cursor empty selection requires no field: %w", ErrQueryLimit)
			}
			if request.Selection.Scope.SeriesField == "" {
				all = true
				total += segment.handle.count
			} else {
				candidates, candidateErr = seriesCandidates(ctx, segment, request.Selection.Scope)
				if candidateErr != nil {
					return nil, candidateErr
				}
				total += candidates.GetCardinality()
			}
		default:
			var err error
			candidates, err = exactCandidates(ctx, segment, request.Selection.Field, request.Selection.Terms,
				request.Selection.Mode, request.Selection.Scope, 0)
			if err != nil {
				return nil, err
			}
			total += candidates.GetCardinality()
		}
		if request.Selection.MaxCandidates != 0 && total > request.Selection.MaxCandidates {
			return nil, ErrQueryLimit
		}
		if request.Range != nil && request.Range.MaxCandidates != 0 && total > request.Range.MaxCandidates {
			return nil, ErrQueryLimit
		}
		cursor.segments = append(cursor.segments, sortCursorSegment{
			segment: segment, segmentID: uint64(index), candidates: candidates, all: all,
		})
	}
	return cursor, nil
}

// NextPage returns the next ordered page and advances its keyset position.
// Cancellation is checked between candidates and leaves the cursor closeable.
func (c *SortCursor) NextPage(ctx context.Context) ([]SortedHit, error) {
	if c == nil || c.closed {
		return nil, errors.New("native: sort cursor closed")
	}
	if err := c.view.check(ctx); err != nil {
		return nil, err
	}
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	frontier := &sortedFrontier{items: make([]sortableHit, 0, c.pageSize)}
	for _, current := range c.segments {
		var documentNumber uint64
		var hasDocument func() bool
		var nextDocument func() uint64
		if current.all {
			var ordinal uint64
			hasDocument = func() bool { return ordinal < current.segment.handle.count }
			nextDocument = func() uint64 {
				value := ordinal
				ordinal++
				return value
			}
		} else {
			iterator := current.candidates.Iterator()
			hasDocument = iterator.HasNext
			nextDocument = func() uint64 { return uint64(iterator.Next()) }
		}
		for hasDocument() {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := c.ctx.Err(); err != nil {
				return nil, err
			}
			documentNumber = nextDocument()
			hit, value, missing, err := c.project(current.segmentID, current.segment, documentNumber)
			if err != nil {
				if errors.Is(err, errSkipSortCandidate) {
					continue
				}
				return nil, err
			}
			item := sortableHit{hit: hit, value: value, missing: missing, desc: c.desc}
			if c.after != nil && !sortKeyAfter(c.after, item) {
				continue
			}
			if len(frontier.items) < c.pageSize {
				heap.Push(frontier, item)
			} else if item.less(frontier.items[0]) {
				heap.Pop(frontier)
				heap.Push(frontier, item)
			}
		}
	}
	ordered := frontier.items
	sortSortableHits(ordered)
	result := make([]SortedHit, len(ordered))
	for index, item := range ordered {
		result[index] = SortedHit{QueryHit: item.hit, SortValue: append([]byte(nil), item.value...), Missing: item.missing}
	}
	if len(result) > 0 {
		last := result[len(result)-1]
		c.after = &SortKey{
			Value: append([]byte(nil), last.SortValue...), Missing: last.Missing,
			Identifier: append([]byte(nil), last.Identifier...), Segment: last.Segment,
			DocumentNumber: last.DocumentNumber,
		}
	}
	return result, nil
}

// Close releases cursor scratch and is idempotent. The caller still owns the
// pinned ReadView and must close it separately.
func (c *SortCursor) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	c.segments = nil
	c.after = nil
	return nil
}

func (c *SortCursor) project(segmentID uint64, segment *memorySegment, documentNumber uint64) (QueryHit, []byte, bool, error) {
	if _, deleted := segment.deleted[documentNumber]; deleted {
		return QueryHit{}, nil, true, errSkipSortCandidate
	}
	var identifier []byte
	var seriesID []byte
	var timestamp int64
	var timestampErr error
	var hasTimestamp bool
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
	if errors.Is(visitErr, errSkipSortCandidate) {
		return QueryHit{}, nil, true, visitErr
	}
	if visitErr != nil {
		return QueryHit{}, nil, false, visitErr
	}
	if timestampErr != nil {
		return QueryHit{}, nil, false, fmt.Errorf("decode timestamp: %w", timestampErr)
	}
	if identifier == nil {
		return QueryHit{}, nil, false, fmt.Errorf("document %d has no identifier: %w", documentNumber, ErrCorrupt)
	}
	if c.timeRange != nil && (!hasTimestamp || !c.timeRange.contains(timestamp)) {
		return QueryHit{}, nil, true, errSkipSortCandidate
	}
	values, valuesErr := segment.handle.reader.DocumentValues(c.sortField, documentNumber)
	if valuesErr != nil {
		return QueryHit{}, nil, false, valuesErr
	}
	value, missing := smallestDocValue(values)
	return QueryHit{Identifier: identifier, SeriesID: seriesID, Timestamp: timestamp, Segment: segmentID, DocumentNumber: documentNumber}, value, missing, nil
}

var errSkipSortCandidate = errors.New("native: skip sort candidate")

type sortedFrontier struct{ items []sortableHit }

func (h sortedFrontier) Len() int           { return len(h.items) }
func (h sortedFrontier) Less(i, j int) bool { return h.items[j].less(h.items[i]) }
func (h sortedFrontier) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *sortedFrontier) Push(value any)    { h.items = append(h.items, value.(sortableHit)) }
func (h *sortedFrontier) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}

func sortSortableHits(items []sortableHit) {
	sort.Slice(items, func(i, j int) bool { return items[i].less(items[j]) })
}

func sortKeyAfter(after *SortKey, item sortableHit) bool {
	if after.Missing != item.missing {
		return !after.Missing && item.missing
	}
	if !after.Missing && !bytes.Equal(after.Value, item.value) {
		if item.desc {
			return bytes.Compare(item.value, after.Value) < 0
		}
		return bytes.Compare(item.value, after.Value) > 0
	}
	if !bytes.Equal(after.Identifier, item.hit.Identifier) {
		return bytes.Compare(item.hit.Identifier, after.Identifier) > 0
	}
	if item.hit.Segment != after.Segment {
		return item.hit.Segment > after.Segment
	}
	return item.hit.DocumentNumber > after.DocumentNumber
}
