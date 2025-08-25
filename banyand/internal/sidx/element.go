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

// Package sidx provides secondary index functionality for BanyanDB, including
// element management, pooling, and sorting capabilities for efficient data storage
// and retrieval operations.
package sidx

import (
	"github.com/apache/skywalking-banyandb/api/common"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	maxPooledSliceSize = 1024 * 1024 // 1MB
)

// tag represents an individual tag (not tag family like stream).
type tag struct {
	name      string
	value     []byte
	valueType pbv1.ValueType
	indexed   bool
}

// elements is a collection of elements optimized for batch operations.
type elements struct {
	seriesIDs []common.SeriesID // Pooled slice
	userKeys  []int64           // Pooled slice (replaces timestamps)
	data      [][]byte          // Pooled slice of slices
	tags      [][]tag           // Pooled slice of tag slices

	// Pool management
	pooled bool // Whether from pool
}

// reset clears tag for reuse.
func (t *tag) reset() {
	t.name = ""
	t.value = nil
	t.valueType = pbv1.ValueTypeUnknown
	t.indexed = false
}

// reset elements collection for pooling.
func (e *elements) reset() {
	e.seriesIDs = e.seriesIDs[:0]
	e.userKeys = e.userKeys[:0]
	// Reset data slices
	for i := range e.data {
		e.data[i] = nil
	}
	e.data = e.data[:0]
	// Reset tag slices
	for i := range e.tags {
		for j := range e.tags[i] {
			e.tags[i][j].reset()
		}
		e.tags[i] = e.tags[i][:0]
	}
	e.tags = e.tags[:0]
	e.pooled = false
}

// size returns the size of the tag in bytes.
func (t *tag) size() int {
	return len(t.name) + len(t.value) + 1 // +1 for valueType
}

// size returns the total size of all elements.
func (e *elements) size() int {
	size := len(e.seriesIDs) * 8
	size += len(e.userKeys) * 8
	for i := range e.data {
		size += len(e.data[i])
	}
	for i := range e.tags {
		for j := range e.tags[i] {
			size += e.tags[i][j].size()
		}
	}
	return size
}

// Implement sort.Interface for elements.
func (e *elements) Len() int {
	return len(e.seriesIDs)
}

func (e *elements) Less(i, j int) bool {
	if e.seriesIDs[i] != e.seriesIDs[j] {
		return e.seriesIDs[i] < e.seriesIDs[j]
	}
	return e.userKeys[i] < e.userKeys[j] // Pure numerical comparison
}

func (e *elements) Swap(i, j int) {
	e.seriesIDs[i], e.seriesIDs[j] = e.seriesIDs[j], e.seriesIDs[i]
	e.userKeys[i], e.userKeys[j] = e.userKeys[j], e.userKeys[i]
	e.data[i], e.data[j] = e.data[j], e.data[i]
	e.tags[i], e.tags[j] = e.tags[j], e.tags[i]
}

var (
	elementsPool = pool.Register[*elements]("sidx-elements")
	tagPool      = pool.Register[*tag]("sidx-tag")
)

// generateElements gets elements collection from pool.
func generateElements() *elements {
	v := elementsPool.Get()
	if v == nil {
		return &elements{pooled: true}
	}
	v.pooled = true
	return v
}

// releaseElements returns elements to pool after reset.
func releaseElements(e *elements) {
	if e == nil || !e.pooled {
		return
	}
	e.reset()
	elementsPool.Put(e)
}

// generateTag gets a tag from pool.
func generateTag() *tag {
	v := tagPool.Get()
	if v == nil {
		return &tag{}
	}
	return v
}

// releaseTag returns tag to pool after reset.
func releaseTag(t *tag) {
	if t == nil {
		return
	}
	t.reset()
	tagPool.Put(t)
}

// mustAppend adds a new element to the collection.
func (e *elements) mustAppend(seriesID common.SeriesID, userKey int64, data []byte, tags []Tag) {
	e.seriesIDs = append(e.seriesIDs, seriesID)
	e.userKeys = append(e.userKeys, userKey)

	// Copy data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	e.data = append(e.data, dataCopy)

	// Convert and copy tags
	elementTags := make([]tag, len(tags))
	for i, t := range tags {
		elementTags[i] = tag{
			name:      t.name,
			value:     append([]byte(nil), t.value...),
			valueType: t.valueType,
			indexed:   t.indexed,
		}
	}
	e.tags = append(e.tags, elementTags)
}
