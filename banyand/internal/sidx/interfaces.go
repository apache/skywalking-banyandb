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
	Stats(ctx context.Context) (Stats, error)

	// Close gracefully shuts down the SIDX instance, ensuring all data is persisted.
	Close() error

	// User-triggered operations for manual control
	Flusher
	Merger
}

// Flusher provides user-triggered persistence operations.
// Users control when memory parts are flushed to disk for durability.
type Flusher interface {
	// Flush triggers persistence of memory parts to disk.
	// Returns error if flush operation fails.
	Flush() error
}

// Merger provides user-triggered compaction operations.
// Users control when parts are merged to optimize storage and query performance.
type Merger interface {
	// Merge triggers compaction of parts to optimize storage.
	// Returns error if merge operation fails.
	Merge() error
}

// Writer handles write path operations for batch processing.
type Writer interface {
	// Write performs batch write operations.
	Write(ctx context.Context, reqs []WriteRequest) error
}

// Querier handles query path operations with filtering and range selection.
type Querier interface {
	// Query executes a query with specified parameters.
	// Returns a QueryResult for iterating over results.
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
	// SeriesID identifies the data series for this element
	SeriesID common.SeriesID

	// Key is the user-provided ordering value (opaque to sidx)
	// sidx only uses this for numerical comparison and ordering
	Key int64

	// Data contains the user payload data
	Data []byte

	// Tags contains individual tags (not tag families like stream module)
	Tags []Tag
}

// QueryRequest specifies parameters for a query operation, following StreamQueryOptions pattern.
type QueryRequest struct {
	// Name identifies the series/index to query
	Name string

	// Entities specifies entity filtering (same as StreamQueryOptions)
	Entities [][]*modelv1.TagValue

	// InvertedFilter for key range and tag-based filtering using index.Filter
	InvertedFilter index.Filter

	// SkippingFilter for additional filtering (following stream pattern)
	SkippingFilter index.Filter

	// Order specifies result ordering using existing index.OrderBy
	Order *index.OrderBy

	// TagProjection specifies which tags to include
	TagProjection []model.TagProjection

	// MaxElementSize limits result size
	MaxElementSize int
}

// QueryResponse contains a batch of query results and execution metadata.
// This follows BanyanDB result patterns with parallel arrays for efficiency.
type QueryResponse struct {
	// Error contains any error that occurred during this batch of query execution.
	// Non-nil Error indicates partial or complete failure during result iteration.
	// Query setup errors are returned by Query() method directly.
	Error error

	// Keys contains the user-provided ordering keys for each result
	Keys []int64

	// Data contains the user payload data for each result
	Data [][]byte

	// TagFamilies contains tag data organized by tag families
	TagFamilies []model.TagFamily

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
	qr.TagFamilies = qr.TagFamilies[:0]
	qr.SIDs = qr.SIDs[:0]
	qr.Metadata = ResponseMetadata{}
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
	QueryCount int64

	// WriteCount tracks total write operations
	WriteCount int64

	// LastFlushTime tracks when last flush occurred
	LastFlushTime int64

	// LastMergeTime tracks when last merge occurred
	LastMergeTime int64
}

// ResponseMetadata provides query execution information for monitoring and debugging.
type ResponseMetadata struct {
	// ExecutionTimeMs is the total query execution time
	ExecutionTimeMs int64

	// ElementsScanned is the total number of elements examined
	ElementsScanned int64

	// ElementsFiltered is the number of elements filtered out
	ElementsFiltered int64

	// PartsAccessed is the number of parts touched during query
	PartsAccessed int

	// BlocksScanned is the number of blocks read
	BlocksScanned int

	// CacheHitRatio indicates cache effectiveness (0.0 to 1.0)
	CacheHitRatio float64

	// Warnings contains any data quality or performance warnings
	Warnings []string

	// TruncatedResults indicates if results were truncated due to limits
	TruncatedResults bool
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
	return nil
}

// Validate validates a QueryRequest for correctness.
func (qr QueryRequest) Validate() error {
	if qr.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	return nil
}

// Reset resets the QueryRequest to its zero state.
func (qr *QueryRequest) Reset() {
	qr.Name = ""
	qr.Entities = nil
	qr.InvertedFilter = nil
	qr.SkippingFilter = nil
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

	qr.InvertedFilter = other.InvertedFilter
	qr.SkippingFilter = other.SkippingFilter
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