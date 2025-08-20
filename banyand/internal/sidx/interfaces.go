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

// Package sidx provides interface definitions for the Secondary Index File System,
// enabling efficient secondary indexing with user-controlled int64 ordering keys.
package sidx

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// SIDX defines the main secondary index interface with user-controlled ordering.
// The core principle is that int64 keys are provided by users and treated as
// opaque ordering values by sidx - the system only performs numerical comparisons
// without interpreting the semantic meaning of keys.
type SIDX interface {
	// Write performs batch write operations. All writes must be submitted as batches.
	// Elements within each batch should be pre-sorted by the caller for optimal performance.
	Write(ctx context.Context, reqs []WriteRequest) error

	// Query executes a query with key range and tag filtering.
	// Returns a QueryResult for iterating over results, following BanyanDB pattern.
	// The returned error indicates query setup/validation failures.
	// Execution errors during result iteration are available in QueryResponse.Error.
	Query(ctx context.Context, req QueryRequest) (QueryResult, error)

	// Stats returns current system statistics and performance metrics.
	Stats(ctx context.Context) (*Stats, error)

	// Close gracefully shuts down the SIDX instance, ensuring all data is persisted.
	Close() error

	// User-triggered operations for manual control
	Flusher
	Merger
}

// Flusher provides user-triggered persistence operations.
// Users control when memory parts are flushed to disk for durability.
// This interface abstracts flush functionality to enable testing and modularity.
type Flusher interface {
	// Flush triggers persistence of memory parts to disk.
	// The implementation should select appropriate memory parts, persist them to disk files,
	// and coordinate with the introducer loop for snapshot updates.
	// This operation is user-controlled and synchronous.
	// Returns error if flush operation fails.
	Flush() error
}

// Merger provides user-triggered compaction operations.
// Users control when parts are merged to optimize storage and query performance.
// This interface abstracts merge functionality to enable testing and modularity.
type Merger interface {
	// Merge triggers compaction of parts to optimize storage.
	// The implementation should select appropriate parts for merging, combine them while maintaining key order,
	// and coordinate with the introducer loop for snapshot updates.
	// This operation is user-controlled and synchronous.
	// Returns error if merge operation fails.
	Merge() error
}

// Writer handles write path operations for batch processing.
// This interface abstracts the write functionality to enable testing and modularity.
type Writer interface {
	// Write performs batch write operations on multiple elements.
	// All elements in the batch should be pre-sorted by seriesID then userKey for optimal performance.
	// The implementation should handle element accumulation, validation, and coordination with memory parts.
	// Returns error if any element in the batch fails validation or if write operation fails.
	Write(ctx context.Context, reqs []WriteRequest) error
}

// Querier handles query path operations with filtering and range selection.
// This interface abstracts the query functionality to enable testing and modularity.
type Querier interface {
	// Query executes a query with specified parameters including key ranges and tag filters.
	// The implementation should handle snapshot access, part filtering, block scanning, and result assembly.
	// Returns a QueryResult for iterating over results following the BanyanDB pattern.
	// The returned error indicates query setup/validation failures.
	// Execution errors during result iteration are available in QueryResponse.Error.
	Query(ctx context.Context, req QueryRequest) (QueryResult, error)
}

// QueryResult provides iterator-like access to query results, following BanyanDB pattern.
type QueryResult interface {
	// Pull returns the next batch of query results.
	// Returns nil when no more results are available.
	// Check QueryResponse.Error for execution errors during iteration.
	Pull() *QueryResponse

	// Release releases resources associated with the query result.
	// Must be called when done with the QueryResult to prevent resource leaks.
	Release()
}

// WriteRequest contains data for a single write operation within a batch.
// The user provides the ordering key as an int64 value that sidx treats opaquely.
type WriteRequest struct {
	Data     []byte
	Tags     []Tag
	SeriesID common.SeriesID
	Key      int64
}

// QueryRequest specifies parameters for a query operation, following StreamQueryOptions pattern.
type QueryRequest struct {
	// Name identifies the series/index to query
	Name string

	// Entities specifies entity filtering (same as StreamQueryOptions)
	Entities [][]*modelv1.TagValue

	// Filter for key range and tag-based filtering using index.Filter
	// Note: sidx uses bloom filters for tag filtering, not inverted indexes
	Filter index.Filter

	// Order specifies result ordering using existing index.OrderBy
	Order *index.OrderBy

	// TagProjection specifies which tags to include
	TagProjection []model.TagProjection

	// MaxElementSize limits result size
	MaxElementSize int
}

// QueryResponse contains a batch of query results and execution metadata.
// This follows BanyanDB result patterns with parallel arrays for efficiency.
// Uses individual tag-based strategy (like trace module) rather than tag-family approach (like stream module).
type QueryResponse struct {
	// Error contains any error that occurred during this batch of query execution.
	// Non-nil Error indicates partial or complete failure during result iteration.
	// Query setup errors are returned by Query() method directly.
	Error error

	// Keys contains the user-provided ordering keys for each result
	Keys []int64

	// Data contains the user payload data for each result
	Data [][]byte

	// Tags contains individual tag data for each result
	Tags [][]Tag

	// SIDs contains the series IDs for each result
	SIDs []common.SeriesID

	// Metadata provides query execution information for this batch
	Metadata ResponseMetadata
}

// Len returns the number of results in the QueryResponse.
func (qr *QueryResponse) Len() int {
	return len(qr.Keys)
}

// Reset resets the QueryResponse to its zero state for reuse.
func (qr *QueryResponse) Reset() {
	qr.Error = nil
	qr.Keys = qr.Keys[:0]
	qr.Data = qr.Data[:0]
	qr.Tags = qr.Tags[:0]
	qr.SIDs = qr.SIDs[:0]
	qr.Metadata = ResponseMetadata{}
}

// Validate validates a QueryResponse for correctness.
func (qr *QueryResponse) Validate() error {
	keysLen := len(qr.Keys)
	dataLen := len(qr.Data)
	sidsLen := len(qr.SIDs)

	if keysLen != dataLen {
		return fmt.Errorf("inconsistent array lengths: keys=%d, data=%d", keysLen, dataLen)
	}
	if keysLen != sidsLen {
		return fmt.Errorf("inconsistent array lengths: keys=%d, sids=%d", keysLen, sidsLen)
	}

	// Validate Tags structure if present
	if len(qr.Tags) > 0 {
		if len(qr.Tags) != keysLen {
			return fmt.Errorf("tags length=%d, expected=%d", len(qr.Tags), keysLen)
		}
		for i, tagGroup := range qr.Tags {
			for j, tag := range tagGroup {
				if tag.name == "" {
					return fmt.Errorf("tags[%d][%d] name cannot be empty", i, j)
				}
			}
		}
	}

	return nil
}

// CopyFrom copies the QueryResponse from other to qr.
func (qr *QueryResponse) CopyFrom(other *QueryResponse) {
	qr.Error = other.Error

	// Copy parallel arrays
	qr.Keys = append(qr.Keys[:0], other.Keys...)
	qr.SIDs = append(qr.SIDs[:0], other.SIDs...)

	// Deep copy data
	if cap(qr.Data) < len(other.Data) {
		qr.Data = make([][]byte, len(other.Data))
	} else {
		qr.Data = qr.Data[:len(other.Data)]
	}
	for i, data := range other.Data {
		qr.Data[i] = append(qr.Data[i][:0], data...)
	}

	// Deep copy tags
	if cap(qr.Tags) < len(other.Tags) {
		qr.Tags = make([][]Tag, len(other.Tags))
	} else {
		qr.Tags = qr.Tags[:len(other.Tags)]
	}
	for i, tagGroup := range other.Tags {
		if cap(qr.Tags[i]) < len(tagGroup) {
			qr.Tags[i] = make([]Tag, len(tagGroup))
		} else {
			qr.Tags[i] = qr.Tags[i][:len(tagGroup)]
		}
		for j, tag := range tagGroup {
			qr.Tags[i][j].name = tag.name
			qr.Tags[i][j].value = append(qr.Tags[i][j].value[:0], tag.value...)
			qr.Tags[i][j].valueType = tag.valueType
			qr.Tags[i][j].indexed = tag.indexed
		}
	}

	// Copy metadata
	qr.Metadata = other.Metadata
}

// Stats contains system statistics and performance metrics.
type Stats struct {
	// MemoryUsageBytes tracks current memory usage
	MemoryUsageBytes int64

	// DiskUsageBytes tracks current disk usage
	DiskUsageBytes int64

	// ElementCount tracks total number of elements
	ElementCount int64

	// PartCount tracks number of parts (memory + disk)
	PartCount int64

	// QueryCount tracks total queries executed
	QueryCount atomic.Int64

	// WriteCount tracks total write operations
	WriteCount atomic.Int64

	// LastFlushTime tracks when last flush occurred
	LastFlushTime int64

	// LastMergeTime tracks when last merge occurred
	LastMergeTime int64
}

// ResponseMetadata provides query execution information for monitoring and debugging.
type ResponseMetadata struct {
	Warnings         []string
	ExecutionTimeMs  int64
	ElementsScanned  int64
	ElementsFiltered int64
	PartsAccessed    int
	BlocksScanned    int
	CacheHitRatio    float64
	TruncatedResults bool
}

// Validate validates ResponseMetadata for correctness.
func (rm *ResponseMetadata) Validate() error {
	if rm.ExecutionTimeMs < 0 {
		return fmt.Errorf("executionTimeMs cannot be negative")
	}
	if rm.ElementsScanned < 0 {
		return fmt.Errorf("elementsScanned cannot be negative")
	}
	if rm.ElementsFiltered < 0 {
		return fmt.Errorf("elementsFiltered cannot be negative")
	}
	if rm.ElementsFiltered > rm.ElementsScanned {
		return fmt.Errorf("elementsFiltered (%d) cannot exceed elementsScanned (%d)", rm.ElementsFiltered, rm.ElementsScanned)
	}
	if rm.PartsAccessed < 0 {
		return fmt.Errorf("partsAccessed cannot be negative")
	}
	if rm.BlocksScanned < 0 {
		return fmt.Errorf("blocksScanned cannot be negative")
	}
	if rm.CacheHitRatio < 0.0 || rm.CacheHitRatio > 1.0 {
		return fmt.Errorf("cacheHitRatio must be between 0.0 and 1.0, got %f", rm.CacheHitRatio)
	}
	return nil
}

// Tag represents an individual tag for WriteRequest.
// This uses the existing tag structure from the sidx package.
type Tag = tag

// Validate validates a WriteRequest for correctness.
func (wr WriteRequest) Validate() error {
	if wr.SeriesID == 0 {
		return fmt.Errorf("seriesID cannot be zero")
	}
	if wr.Data == nil {
		return fmt.Errorf("data cannot be nil")
	}
	if len(wr.Data) == 0 {
		return fmt.Errorf("data cannot be empty")
	}
	// Validate tags if present
	for i, tag := range wr.Tags {
		if tag.name == "" {
			return fmt.Errorf("tag[%d] name cannot be empty", i)
		}
		if len(tag.value) == 0 {
			return fmt.Errorf("tag[%d] value cannot be empty", i)
		}
	}
	return nil
}

// Validate validates a QueryRequest for correctness.
func (qr QueryRequest) Validate() error {
	if qr.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if qr.MaxElementSize < 0 {
		return fmt.Errorf("maxElementSize cannot be negative")
	}
	// Validate tag projection names
	for i, projection := range qr.TagProjection {
		if projection.Family == "" {
			return fmt.Errorf("tagProjection[%d] family cannot be empty", i)
		}
	}
	// Validate entities structure
	for i, entityGroup := range qr.Entities {
		if len(entityGroup) == 0 {
			return fmt.Errorf("entities[%d] cannot be empty", i)
		}
		for j, tagValue := range entityGroup {
			if tagValue == nil {
				return fmt.Errorf("entities[%d][%d] cannot be nil", i, j)
			}
		}
	}
	return nil
}

// Reset resets the QueryRequest to its zero state.
func (qr *QueryRequest) Reset() {
	qr.Name = ""
	qr.Entities = nil
	qr.Filter = nil
	qr.Order = nil
	qr.TagProjection = nil
	qr.MaxElementSize = 0
}

// CopyFrom copies the QueryRequest from other to qr.
func (qr *QueryRequest) CopyFrom(other *QueryRequest) {
	qr.Name = other.Name

	// Deep copy for Entities if it's a slice
	if other.Entities != nil {
		qr.Entities = make([][]*modelv1.TagValue, len(other.Entities))
		copy(qr.Entities, other.Entities)
	} else {
		qr.Entities = nil
	}

	qr.Filter = other.Filter
	qr.Order = other.Order

	// Deep copy if TagProjection is a slice
	if other.TagProjection != nil {
		qr.TagProjection = make([]model.TagProjection, len(other.TagProjection))
		copy(qr.TagProjection, other.TagProjection)
	} else {
		qr.TagProjection = nil
	}

	qr.MaxElementSize = other.MaxElementSize
}

// Interface Usage Examples and Best Practices
//
// These examples demonstrate how the component interfaces work together and can be used
// independently for testing, mocking, and modular implementations.

// Example: Using Writer interface independently
//
//	writer := NewWriter(options)
//	reqs := []WriteRequest{
//		{SeriesID: 1, Key: 100, Data: []byte("data1")},
//		{SeriesID: 1, Key: 101, Data: []byte("data2")},
//	}
//	if err := writer.Write(ctx, reqs); err != nil {
//		log.Fatalf("write failed: %v", err)
//	}

// Example: Using Querier interface independently
//
//	querier := NewQuerier(options)
//	req := QueryRequest{
//		Name: "my-index",
//		Filter: createKeyRangeFilter(100, 200),
//		Order: &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
//	}
//	result, err := querier.Query(ctx, req)
//	if err != nil {
//		log.Fatalf("query setup failed: %v", err)
//	}
//	defer result.Release()
//
//	for {
//		batch := result.Pull()
//		if batch == nil {
//			break // No more results
//		}
//		if batch.Error != nil {
//			log.Printf("query execution error: %v", batch.Error)
//		}
//		// Process batch.Keys, batch.Data, batch.Tags, etc.
//	}

// Example: Interface composition in SIDX
//
//	type sidxImpl struct {
//		writer  Writer
//		querier Querier
//		flusher Flusher
//		merger  Merger
//	}
//
//	func (s *sidxImpl) Write(ctx context.Context, reqs []WriteRequest) error {
//		return s.writer.Write(ctx, reqs)
//	}
//
//	func (s *sidxImpl) Query(ctx context.Context, req QueryRequest) (QueryResult, error) {
//		return s.querier.Query(ctx, req)
//	}
//
//	func (s *sidxImpl) Flush() error {
//		return s.flusher.Flush()
//	}
//
//	func (s *sidxImpl) Merge() error {
//		return s.merger.Merge()
//	}

// Example: Mock implementations for testing
//
//	type mockWriter struct {
//		writeFunc func(context.Context, []WriteRequest) error
//	}
//
//	func (m *mockWriter) Write(ctx context.Context, reqs []WriteRequest) error {
//		if m.writeFunc != nil {
//			return m.writeFunc(ctx, reqs)
//		}
//		return nil
//	}
//
//	// Test usage
//	writer := &mockWriter{
//		writeFunc: func(ctx context.Context, reqs []WriteRequest) error {
//			// Custom test logic
//			return nil
//		},
//	}

//nolint:godot
// Interface Design Principles.
//
// 1. **Single Responsibility**: Each interface has a focused, well-defined purpose.
// 2. **Minimal Surface Area**: Interfaces expose only essential methods
// 3. **Composability**: Interfaces can be combined to create larger systems
// 4. **Testability**: Small interfaces are easy to mock and test
// 5. **Modularity**: Implementations can be swapped independently
// 6. **Documentation**: Clear contracts and usage examples
//
// Interface Decoupling Benefits:
// - Independent testing of components
// - Easy mocking for unit tests
// - Flexible implementation strategies
// - Clear separation of concerns
// - Simplified dependency injection
