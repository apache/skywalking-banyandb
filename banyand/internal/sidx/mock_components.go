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

package sidx

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
)

// MockWriter provides a mock implementation of the Writer interface
// for testing and early integration development.
type MockWriter struct {
	elements      []WriteRequest
	config        MockWriterConfig
	writeCount    atomic.Int64
	elementCount  atomic.Int64
	lastWriteTime int64
	mu            sync.RWMutex
}

// MockWriterConfig provides configuration options for the mock writer.
type MockWriterConfig struct {
	// DelayMs introduces artificial delay for write operations
	DelayMs int
	// ErrorRate controls the percentage of operations that should fail (0-100)
	ErrorRate int
	// MaxElements limits the number of elements to prevent OOM in tests
	MaxElements int
	// EnableValidation enables strict request validation
	EnableValidation bool
	// SortElements automatically sorts elements by SeriesID then Key
	SortElements bool
}

// DefaultMockWriterConfig returns a sensible default configuration.
func DefaultMockWriterConfig() MockWriterConfig {
	return MockWriterConfig{
		DelayMs:          0,
		ErrorRate:        0,
		MaxElements:      50000,
		EnableValidation: true,
		SortElements:     true,
	}
}

// NewMockWriter creates a new mock writer with the given configuration.
func NewMockWriter(config MockWriterConfig) *MockWriter {
	return &MockWriter{
		elements: make([]WriteRequest, 0),
		config:   config,
	}
}

// Write implements the Writer interface with element accumulation.
func (mw *MockWriter) Write(_ context.Context, reqs []WriteRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	// Simulate artificial delay if configured
	if mw.config.DelayMs > 0 {
		time.Sleep(time.Duration(mw.config.DelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if mw.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < mw.config.ErrorRate {
		return fmt.Errorf("mock writer error injection: write failed")
	}

	// Validate requests if enabled
	if mw.config.EnableValidation {
		for i, req := range reqs {
			if err := req.Validate(); err != nil {
				return fmt.Errorf("invalid write request[%d]: %w", i, err)
			}
		}
	}

	mw.mu.Lock()
	defer mw.mu.Unlock()

	// Check element limit
	if mw.config.MaxElements > 0 && len(mw.elements)+len(reqs) > mw.config.MaxElements {
		return fmt.Errorf("element limit exceeded: current=%d, adding=%d, limit=%d",
			len(mw.elements), len(reqs), mw.config.MaxElements)
	}

	// Accumulate elements
	mw.elements = append(mw.elements, reqs...)

	// Sort elements if configured
	if mw.config.SortElements {
		sort.Slice(mw.elements, func(i, j int) bool {
			if mw.elements[i].SeriesID != mw.elements[j].SeriesID {
				return mw.elements[i].SeriesID < mw.elements[j].SeriesID
			}
			return mw.elements[i].Key < mw.elements[j].Key
		})
	}

	// Update stats
	mw.writeCount.Add(1)
	mw.elementCount.Add(int64(len(reqs)))
	atomic.StoreInt64(&mw.lastWriteTime, time.Now().UnixNano())

	return nil
}

// GetElements returns a copy of all accumulated elements (for testing).
func (mw *MockWriter) GetElements() []WriteRequest {
	mw.mu.RLock()
	defer mw.mu.RUnlock()

	result := make([]WriteRequest, len(mw.elements))
	copy(result, mw.elements)
	return result
}

// GetStats returns writer statistics.
func (mw *MockWriter) GetStats() (writeCount, elementCount int64, lastWriteTime time.Time) {
	lastWriteNanos := atomic.LoadInt64(&mw.lastWriteTime)
	if lastWriteNanos == 0 {
		return mw.writeCount.Load(), mw.elementCount.Load(), time.Time{}
	}
	return mw.writeCount.Load(), mw.elementCount.Load(), time.Unix(0, lastWriteNanos)
}

// Clear removes all accumulated elements (for testing).
func (mw *MockWriter) Clear() {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	mw.elements = mw.elements[:0]
	mw.elementCount.Store(0)
}

// SetErrorRate allows dynamic configuration of error injection rate.
func (mw *MockWriter) SetErrorRate(rate int) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	if rate >= 0 && rate <= 100 {
		mw.config.ErrorRate = rate
	}
}

// MockQuerier provides a mock implementation of the Querier interface
// for testing and early integration development.
type MockQuerier struct {
	elements      []WriteRequest
	config        MockQuerierConfig
	queryCount    atomic.Int64
	lastQueryTime int64
	mu            sync.RWMutex
}

// MockQuerierConfig provides configuration options for the mock querier.
type MockQuerierConfig struct {
	// DelayMs introduces artificial delay for query operations
	DelayMs int
	// ErrorRate controls the percentage of operations that should fail (0-100)
	ErrorRate int
	// EnableValidation enables strict request validation
	EnableValidation bool
	// MaxResultSize limits the maximum number of results returned
	MaxResultSize int
}

// DefaultMockQuerierConfig returns a sensible default configuration.
func DefaultMockQuerierConfig() MockQuerierConfig {
	return MockQuerierConfig{
		DelayMs:          0,
		ErrorRate:        0,
		EnableValidation: true,
		MaxResultSize:    10000,
	}
}

// NewMockQuerier creates a new mock querier with the given configuration.
func NewMockQuerier(config MockQuerierConfig) *MockQuerier {
	return &MockQuerier{
		elements: make([]WriteRequest, 0),
		config:   config,
	}
}

// SetElements sets the elements available for querying (for testing).
func (mq *MockQuerier) SetElements(elements []WriteRequest) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	mq.elements = make([]WriteRequest, len(elements))
	copy(mq.elements, elements)

	// Ensure elements are sorted for range filtering
	sort.Slice(mq.elements, func(i, j int) bool {
		if mq.elements[i].SeriesID != mq.elements[j].SeriesID {
			return mq.elements[i].SeriesID < mq.elements[j].SeriesID
		}
		return mq.elements[i].Key < mq.elements[j].Key
	})
}

// Query implements the Querier interface with range filtering.
func (mq *MockQuerier) Query(_ context.Context, req QueryRequest) (QueryResult, error) {
	// Simulate artificial delay if configured
	if mq.config.DelayMs > 0 {
		time.Sleep(time.Duration(mq.config.DelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if mq.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < mq.config.ErrorRate {
		return nil, fmt.Errorf("mock querier error injection: query failed")
	}

	// Validate request if enabled
	if mq.config.EnableValidation {
		if err := req.Validate(); err != nil {
			return nil, fmt.Errorf("invalid query request: %w", err)
		}
	}

	mq.mu.RLock()
	defer mq.mu.RUnlock()

	// Update stats
	mq.queryCount.Add(1)
	atomic.StoreInt64(&mq.lastQueryTime, time.Now().UnixNano())

	// Apply basic range filtering (simplified for mock)
	var filteredElements []WriteRequest
	for _, elem := range mq.elements {
		// Simple filtering based on request parameters
		// In a real implementation, this would use proper index filtering
		if mq.matchesQuery(elem, req) {
			filteredElements = append(filteredElements, elem)
			if mq.config.MaxResultSize > 0 && len(filteredElements) >= mq.config.MaxResultSize {
				break
			}
		}
	}

	// Create and return query result
	return &mockQuerierResult{
		elements: filteredElements,
		request:  req,
	}, nil
}

// matchesQuery performs basic filtering to determine if an element matches the query.
func (mq *MockQuerier) matchesQuery(elem WriteRequest, req QueryRequest) bool {
	// Basic range filtering - in reality this would be more sophisticated
	// For the mock, we'll match all elements unless specific filters are applied

	// Apply entity filtering if specified
	if len(req.Entities) > 0 {
		// Simplified entity matching for the mock
		// In practice, this would properly evaluate entity constraints
		_ = elem // Use elem to avoid unused parameter warning
	}

	// For the mock, we'll accept all elements that pass basic checks
	return true
}

// GetStats returns querier statistics.
func (mq *MockQuerier) GetStats() (queryCount int64, lastQueryTime time.Time) {
	lastQueryNanos := atomic.LoadInt64(&mq.lastQueryTime)
	if lastQueryNanos == 0 {
		return mq.queryCount.Load(), time.Time{}
	}
	return mq.queryCount.Load(), time.Unix(0, lastQueryNanos)
}

// SetErrorRate allows dynamic configuration of error injection rate.
func (mq *MockQuerier) SetErrorRate(rate int) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	if rate >= 0 && rate <= 100 {
		mq.config.ErrorRate = rate
	}
}

// mockQuerierResult implements QueryResult for the mock querier.
type mockQuerierResult struct {
	elements []WriteRequest
	request  QueryRequest
	position int
	finished bool
}

// Pull returns the next batch of query results.
func (mqr *mockQuerierResult) Pull() *QueryResponse {
	if mqr.finished || mqr.position >= len(mqr.elements) {
		return nil
	}

	startTime := time.Now()
	batchSize := mqr.request.MaxElementSize
	if batchSize <= 0 {
		batchSize = len(mqr.elements) - mqr.position // Return all remaining
	}

	endPos := mqr.position + batchSize
	if endPos > len(mqr.elements) {
		endPos = len(mqr.elements)
	}

	batch := mqr.elements[mqr.position:endPos]
	mqr.position = endPos

	if mqr.position >= len(mqr.elements) {
		mqr.finished = true
	}

	// Convert batch to response format
	response := &QueryResponse{
		Keys: make([]int64, len(batch)),
		Data: make([][]byte, len(batch)),
		Tags: make([][]tag, len(batch)),
		SIDs: make([]common.SeriesID, len(batch)),
	}

	for i, elem := range batch {
		response.Keys[i] = elem.Key
		response.Data[i] = make([]byte, len(elem.Data))
		copy(response.Data[i], elem.Data)
		response.Tags[i] = make([]tag, len(elem.Tags))
		copy(response.Tags[i], elem.Tags)
		response.SIDs[i] = elem.SeriesID
	}

	// Fill metadata
	response.Metadata = ResponseMetadata{
		ExecutionTimeMs:  time.Since(startTime).Milliseconds(),
		ElementsScanned:  int64(len(batch)),
		ElementsFiltered: int64(len(batch)),
		PartsAccessed:    1,   // Mock has 1 virtual part
		BlocksScanned:    1,   // Mock scans 1 virtual block
		CacheHitRatio:    1.0, // Everything is in "memory cache"
		TruncatedResults: mqr.request.MaxElementSize > 0 && len(batch) == batchSize && !mqr.finished,
	}

	return response
}

// Release releases resources associated with the query result.
func (mqr *mockQuerierResult) Release() {
	mqr.elements = nil
	mqr.finished = true
}

// MockFlusher provides a mock implementation of the Flusher interface
// for testing and early integration development.
type MockFlusher struct {
	mu            sync.RWMutex
	config        MockFlusherConfig
	flushCount    atomic.Int64
	lastFlushTime int64
}

// MockFlusherConfig provides configuration options for the mock flusher.
type MockFlusherConfig struct {
	// DelayMs introduces artificial delay for flush operations
	DelayMs int
	// ErrorRate controls the percentage of operations that should fail (0-100)
	ErrorRate int
	// SimulateWork enables simulation of actual flush work
	SimulateWork bool
}

// DefaultMockFlusherConfig returns a sensible default configuration.
func DefaultMockFlusherConfig() MockFlusherConfig {
	return MockFlusherConfig{
		DelayMs:      50, // Simulate some disk I/O time
		ErrorRate:    0,
		SimulateWork: true,
	}
}

// NewMockFlusher creates a new mock flusher with the given configuration.
func NewMockFlusher(config MockFlusherConfig) *MockFlusher {
	return &MockFlusher{
		config: config,
	}
}

// Flush implements the Flusher interface with no-op operations.
func (mf *MockFlusher) Flush() error {
	// Simulate artificial delay if configured
	if mf.config.DelayMs > 0 {
		time.Sleep(time.Duration(mf.config.DelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if mf.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < mf.config.ErrorRate {
		return fmt.Errorf("mock flusher error injection: flush failed")
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()

	// Simulate flush work if enabled
	if mf.config.SimulateWork {
		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)
	}

	// Update stats
	mf.flushCount.Add(1)
	atomic.StoreInt64(&mf.lastFlushTime, time.Now().UnixNano())

	return nil
}

// GetStats returns flusher statistics.
func (mf *MockFlusher) GetStats() (flushCount int64, lastFlushTime time.Time) {
	lastFlushNanos := atomic.LoadInt64(&mf.lastFlushTime)
	if lastFlushNanos == 0 {
		return mf.flushCount.Load(), time.Time{}
	}
	return mf.flushCount.Load(), time.Unix(0, lastFlushNanos)
}

// SetErrorRate allows dynamic configuration of error injection rate.
func (mf *MockFlusher) SetErrorRate(rate int) {
	mf.mu.Lock()
	defer mf.mu.Unlock()
	if rate >= 0 && rate <= 100 {
		mf.config.ErrorRate = rate
	}
}

// MockMerger provides a mock implementation of the Merger interface
// for testing and early integration development.
type MockMerger struct {
	consolidations []MergeConsolidation
	config         MockMergerConfig
	mergeCount     atomic.Int64
	lastMergeTime  int64
	mu             sync.RWMutex
}

// MergeConsolidation represents a simulated merge operation result.
type MergeConsolidation struct {
	Timestamp         time.Time
	PartsInput        int
	PartsOutput       int
	ElementsProcessed int64
	Duration          time.Duration
}

// MockMergerConfig provides configuration options for the mock merger.
type MockMergerConfig struct {
	// DelayMs introduces artificial delay for merge operations
	DelayMs int
	// ErrorRate controls the percentage of operations that should fail (0-100)
	ErrorRate int
	// SimulateWork enables simulation of actual merge work
	SimulateWork bool
	// ConsolidationRatio simulates the reduction ratio during merge (0.1 to 1.0)
	ConsolidationRatio float64
}

// DefaultMockMergerConfig returns a sensible default configuration.
func DefaultMockMergerConfig() MockMergerConfig {
	return MockMergerConfig{
		DelayMs:            100, // Simulate merge processing time
		ErrorRate:          0,
		SimulateWork:       true,
		ConsolidationRatio: 0.7, // 30% reduction typical
	}
}

// NewMockMerger creates a new mock merger with the given configuration.
func NewMockMerger(config MockMergerConfig) *MockMerger {
	return &MockMerger{
		config:         config,
		consolidations: make([]MergeConsolidation, 0),
	}
}

// Merge implements the Merger interface with simple consolidation.
func (mm *MockMerger) Merge() error {
	startTime := time.Now()

	// Simulate artificial delay if configured
	if mm.config.DelayMs > 0 {
		time.Sleep(time.Duration(mm.config.DelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if mm.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < mm.config.ErrorRate {
		return fmt.Errorf("mock merger error injection: merge failed")
	}

	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Simulate merge work if enabled
	if mm.config.SimulateWork {
		// Simulate processing time
		time.Sleep(20 * time.Millisecond)
	}

	// Simulate consolidation
	inputParts := 3 + (int(time.Now().UnixNano()) % 5) // 3-7 input parts
	outputParts := int(float64(inputParts) * mm.config.ConsolidationRatio)
	if outputParts < 1 {
		outputParts = 1
	}
	elementsProcessed := 1000 + (time.Now().UnixNano() % 5000) // 1000-6000 elements

	consolidation := MergeConsolidation{
		Timestamp:         startTime,
		PartsInput:        inputParts,
		PartsOutput:       outputParts,
		ElementsProcessed: elementsProcessed,
		Duration:          time.Since(startTime),
	}

	mm.consolidations = append(mm.consolidations, consolidation)

	// Keep only last 10 consolidations for memory efficiency
	if len(mm.consolidations) > 10 {
		mm.consolidations = mm.consolidations[len(mm.consolidations)-10:]
	}

	// Update stats
	mm.mergeCount.Add(1)
	atomic.StoreInt64(&mm.lastMergeTime, time.Now().UnixNano())

	return nil
}

// GetStats returns merger statistics.
func (mm *MockMerger) GetStats() (mergeCount int64, lastMergeTime time.Time) {
	lastMergeNanos := atomic.LoadInt64(&mm.lastMergeTime)
	if lastMergeNanos == 0 {
		return mm.mergeCount.Load(), time.Time{}
	}
	return mm.mergeCount.Load(), time.Unix(0, lastMergeNanos)
}

// GetConsolidations returns recent merge consolidation history (for testing).
func (mm *MockMerger) GetConsolidations() []MergeConsolidation {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make([]MergeConsolidation, len(mm.consolidations))
	copy(result, mm.consolidations)
	return result
}

// SetErrorRate allows dynamic configuration of error injection rate.
func (mm *MockMerger) SetErrorRate(rate int) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if rate >= 0 && rate <= 100 {
		mm.config.ErrorRate = rate
	}
}

// MockComponentSuite provides a convenient way to create and manage
// all mock components together for integration testing.
type MockComponentSuite struct {
	Writer  *MockWriter
	Querier *MockQuerier
	Flusher *MockFlusher
	Merger  *MockMerger
}

// NewMockComponentSuite creates a new suite of mock components with default configurations.
func NewMockComponentSuite() *MockComponentSuite {
	return &MockComponentSuite{
		Writer:  NewMockWriter(DefaultMockWriterConfig()),
		Querier: NewMockQuerier(DefaultMockQuerierConfig()),
		Flusher: NewMockFlusher(DefaultMockFlusherConfig()),
		Merger:  NewMockMerger(DefaultMockMergerConfig()),
	}
}

// NewMockComponentSuiteWithConfigs creates a new suite with custom configurations.
func NewMockComponentSuiteWithConfigs(
	writerConfig MockWriterConfig,
	querierConfig MockQuerierConfig,
	flusherConfig MockFlusherConfig,
	mergerConfig MockMergerConfig,
) *MockComponentSuite {
	return &MockComponentSuite{
		Writer:  NewMockWriter(writerConfig),
		Querier: NewMockQuerier(querierConfig),
		Flusher: NewMockFlusher(flusherConfig),
		Merger:  NewMockMerger(mergerConfig),
	}
}

// SyncElements synchronizes elements from writer to querier (for testing workflows).
func (mcs *MockComponentSuite) SyncElements() {
	elements := mcs.Writer.GetElements()
	mcs.Querier.SetElements(elements)
}

// SetGlobalErrorRate sets error rate for all components.
func (mcs *MockComponentSuite) SetGlobalErrorRate(rate int) {
	mcs.Writer.SetErrorRate(rate)
	mcs.Querier.SetErrorRate(rate)
	mcs.Flusher.SetErrorRate(rate)
	mcs.Merger.SetErrorRate(rate)
}

// Reset clears all components to initial state (for testing).
func (mcs *MockComponentSuite) Reset() {
	mcs.Writer.Clear()
	mcs.Querier.SetElements(nil)
}
