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

// Package model defines the structures and interfaces for query options and results.
package model

import (
	"container/heap"
	"context"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const maxTopN = 20

// Tag is a tag name and its values.
type Tag struct {
	Name   string
	Values []*modelv1.TagValue
}

// TagFamily is a tag family name and its tags.
type TagFamily struct {
	Name string
	Tags []Tag
}

// Field is a field name and its values.
type Field struct {
	Name   string
	Values []*modelv1.FieldValue
}

// TagProjection is the projection of a tag family and its tags.
type TagProjection struct {
	Family string
	Names  []string
}

// MeasureQueryOptions is the options of a measure query.
type MeasureQueryOptions struct {
	Query           index.Query
	TimeRange       *timestamp.TimeRange
	Order           *index.OrderBy
	Name            string
	Entities        [][]*modelv1.TagValue
	TagProjection   []TagProjection
	FieldProjection []string
}

// MeasureResult is the result of a query.
type MeasureResult struct {
	Error       error
	Timestamps  []int64
	Versions    []int64
	TagFamilies []TagFamily
	Fields      []Field
	SID         common.SeriesID
}

// MeasureQueryResult is the result of a measure query.
type MeasureQueryResult interface {
	Pull() *MeasureResult
	Release()
}

// StreamQueryOptions is the options of a stream query.
type StreamQueryOptions struct {
	Name           string
	TimeRange      *timestamp.TimeRange
	Entities       [][]*modelv1.TagValue
	InvertedFilter index.Filter
	SkippingFilter index.Filter
	Order          *index.OrderBy
	TagProjection  []TagProjection
	MaxElementSize int
}

// Reset resets the StreamQueryOptions.
func (s *StreamQueryOptions) Reset() {
	s.Name = ""
	s.TimeRange = nil
	s.Entities = nil
	s.InvertedFilter = nil
	s.SkippingFilter = nil
	s.Order = nil
	s.TagProjection = nil
	s.MaxElementSize = 0
}

// CopyFrom copies the StreamQueryOptions from other to s.
func (s *StreamQueryOptions) CopyFrom(other *StreamQueryOptions) {
	s.Name = other.Name
	s.TimeRange = other.TimeRange

	// Deep copy for Entities if it's a slice
	if other.Entities != nil {
		s.Entities = make([][]*modelv1.TagValue, len(other.Entities))
		copy(s.Entities, other.Entities)
	} else {
		s.Entities = nil
	}

	s.InvertedFilter = other.InvertedFilter
	s.SkippingFilter = other.SkippingFilter
	s.Order = other.Order

	// Deep copy if TagProjection is a slice
	if other.TagProjection != nil {
		s.TagProjection = make([]TagProjection, len(other.TagProjection))
		copy(s.TagProjection, other.TagProjection)
	} else {
		s.TagProjection = nil
	}

	s.MaxElementSize = other.MaxElementSize
}

// StreamResult is the result of a query.
type StreamResult struct {
	Error       error
	Timestamps  []int64
	ElementIDs  []uint64
	TagFamilies []TagFamily
	SIDs        []common.SeriesID
	topN        int
	idx         int
	asc         bool
}

// NewStreamResult creates a new StreamResult.
func NewStreamResult(topN int, asc bool) *StreamResult {
	capacity := topN
	if topN > maxTopN {
		capacity = maxTopN
	}
	return &StreamResult{
		topN:        topN,
		asc:         asc,
		Timestamps:  make([]int64, 0, capacity),
		ElementIDs:  make([]uint64, 0, capacity),
		TagFamilies: make([]TagFamily, 0, capacity),
		SIDs:        make([]common.SeriesID, 0, capacity),
	}
}

// Len returns the length of the StreamResult.
func (sr *StreamResult) Len() int {
	return len(sr.Timestamps)
}

// Reset resets the StreamResult.
func (sr *StreamResult) Reset() {
	sr.Error = nil
	sr.idx = 0
	sr.Timestamps = sr.Timestamps[:0]
	sr.ElementIDs = sr.ElementIDs[:0]
	sr.TagFamilies = sr.TagFamilies[:0]
	sr.SIDs = sr.SIDs[:0]
}

// CopyFrom copies the topN results from other to sr using tmp as a temporary result.
func (sr *StreamResult) CopyFrom(tmp, other *StreamResult) bool {
	// Prepare a reusable tmp result
	tmp.Reset()
	tmp.topN = sr.topN
	tmp.asc = sr.asc

	// Prepare heaps
	sr.idx = 0
	other.idx = 0

	h := &StreamResultHeap{asc: sr.asc}
	heap.Init(h)

	if sr.Len() > 0 {
		heap.Push(h, sr)
	}
	if other.Len() > 0 {
		heap.Push(h, other)
	}

	// Pop from heap to build tmp with topN
	for h.Len() > 0 && tmp.Len() < tmp.topN {
		res := heap.Pop(h).(*StreamResult)
		tmp.CopySingleFrom(res)
		res.idx++
		if res.idx < res.Len() {
			heap.Push(h, res)
		}
	}

	// Copy tmp back to sr
	sr.Reset()
	sr.Timestamps = append(sr.Timestamps, tmp.Timestamps...)
	sr.ElementIDs = append(sr.ElementIDs, tmp.ElementIDs...)
	sr.SIDs = append(sr.SIDs, tmp.SIDs...)
	sr.TagFamilies = append(sr.TagFamilies, tmp.TagFamilies...)

	return len(sr.Timestamps) >= sr.topN
}

// CopySingleFrom copies a single result from other to sr.
func (sr *StreamResult) CopySingleFrom(other *StreamResult) {
	sr.SIDs = append(sr.SIDs, other.SIDs[other.idx])
	sr.Timestamps = append(sr.Timestamps, other.Timestamps[other.idx])
	sr.ElementIDs = append(sr.ElementIDs, other.ElementIDs[other.idx])
	if len(sr.TagFamilies) < len(other.TagFamilies) {
		for i := range other.TagFamilies {
			tf := TagFamily{
				Name: other.TagFamilies[i].Name,
				Tags: make([]Tag, len(other.TagFamilies[i].Tags)),
			}
			for j := range tf.Tags {
				tf.Tags[j].Name = other.TagFamilies[i].Tags[j].Name
			}
			sr.TagFamilies = append(sr.TagFamilies, tf)
		}
	}
	if len(sr.TagFamilies) != len(other.TagFamilies) {
		logger.Panicf("tag family length mismatch: %d != %d", len(sr.TagFamilies), len(other.TagFamilies))
	}
	for i := range sr.TagFamilies {
		if len(sr.TagFamilies[i].Tags) != len(other.TagFamilies[i].Tags) {
			logger.Panicf("tag length mismatch: %d != %d", len(sr.TagFamilies[i].Tags), len(other.TagFamilies[i].Tags))
		}
		for j := range sr.TagFamilies[i].Tags {
			sr.TagFamilies[i].Tags[j].Values = append(sr.TagFamilies[i].Tags[j].Values, other.TagFamilies[i].Tags[j].Values[other.idx])
		}
	}
}

var bypassStreamResult = &StreamResult{}

// StreamResultHeap is a min-heap of StreamResult pointers.
type StreamResultHeap struct {
	data []*StreamResult
	asc  bool
}

func (h StreamResultHeap) Len() int { return len(h.data) }
func (h StreamResultHeap) Less(i, j int) bool {
	if h.asc {
		return h.data[i].Timestamps[h.data[i].idx] < h.data[j].Timestamps[h.data[j].idx]
	}
	return h.data[i].Timestamps[h.data[i].idx] > h.data[j].Timestamps[h.data[j].idx]
}
func (h StreamResultHeap) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

// Push pushes a StreamResult pointer to the heap.
func (h *StreamResultHeap) Push(x interface{}) {
	h.data = append(h.data, x.(*StreamResult))
}

// Pop pops a StreamResult pointer from the heap.
func (h *StreamResultHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}

// MergeStreamResults merges multiple StreamResult slices into a single StreamResult.
func MergeStreamResults(results []*StreamResult, topN int, asc bool) *StreamResult {
	h := &StreamResultHeap{asc: asc}
	heap.Init(h)

	for _, result := range results {
		if result.Len() > 0 {
			result.idx = 0
			heap.Push(h, result)
		}
	}

	if h.Len() == 0 {
		return bypassStreamResult
	}

	mergedResult := NewStreamResult(topN, asc)

	for h.Len() > 0 && mergedResult.Len() < topN {
		sr := heap.Pop(h).(*StreamResult)
		mergedResult.CopySingleFrom(sr)
		sr.idx++
		if sr.idx < sr.Len() {
			heap.Push(h, sr)
		}
	}

	return mergedResult
}

// StreamQueryResult is the result of a stream query.
type StreamQueryResult interface {
	Pull(context.Context) *StreamResult
	Release()
}

// TraceQueryOptions is the options of a trace query.
type TraceQueryOptions struct {
	TimeRange      *timestamp.TimeRange
	SkippingFilter index.Filter
	Order          *index.OrderBy
	TagProjection  *TagProjection
	Name           string
	MaxTraceSize   int
}

// Reset resets the TraceQueryOptions.
func (t *TraceQueryOptions) Reset() {
	t.Name = ""
	t.TimeRange = nil
	t.SkippingFilter = nil
	t.Order = nil
	t.TagProjection = nil
	t.MaxTraceSize = 0
}

// CopyFrom copies the TraceQueryOptions from other to t.
func (t *TraceQueryOptions) CopyFrom(other *TraceQueryOptions) {
	t.Name = other.Name
	t.TimeRange = other.TimeRange
	t.SkippingFilter = other.SkippingFilter
	t.Order = other.Order
	t.TagProjection = other.TagProjection
	t.MaxTraceSize = other.MaxTraceSize
}

// TraceResult is the result of a query.
type TraceResult struct {
	Error error
	Spans [][]byte
	TID   string
	Tags  []Tag
}

// TraceQueryResult is the result of a trace query.
type TraceQueryResult interface {
	Pull() *TraceResult
	Release()
}
