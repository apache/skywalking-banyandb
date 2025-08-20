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

// MockSIDX provides an in-memory mock implementation of the SIDX interface
// for early testing and integration development.
type MockSIDX struct {
	storage        map[string][]mockElement
	stats          Stats
	config         MockConfig
	lastFlushTime  int64
	lastMergeTime  int64
	elementCounter atomic.Int64
	mu             sync.RWMutex
	closed         atomic.Bool
}

// mockElement represents a stored element in the mock implementation.
type mockElement struct {
	Data     []byte
	Tags     []tag
	SeriesID common.SeriesID
	Key      int64
}

// MockConfig provides configuration options for the mock implementation.
type MockConfig struct {
	// WriteDelayMs introduces artificial delay for write operations
	WriteDelayMs int
	// QueryDelayMs introduces artificial delay for query operations
	QueryDelayMs int
	// FlushDelayMs introduces artificial delay for flush operations
	FlushDelayMs int
	// MergeDelayMs introduces artificial delay for merge operations
	MergeDelayMs int
	// ErrorRate controls the percentage of operations that should fail (0-100)
	ErrorRate int
	// MaxElements limits the number of elements to prevent OOM in tests
	MaxElements int
	// EnableStrictValidation enables additional validation checks
	EnableStrictValidation bool
}

// DefaultMockConfig returns a sensible default configuration for the mock.
func DefaultMockConfig() MockConfig {
	return MockConfig{
		WriteDelayMs:           0,
		QueryDelayMs:           0,
		FlushDelayMs:           10,
		MergeDelayMs:           20,
		ErrorRate:              0,
		MaxElements:            100000,
		EnableStrictValidation: true,
	}
}

// NewMockSIDX creates a new mock SIDX implementation with the given configuration.
func NewMockSIDX(config MockConfig) *MockSIDX {
	return &MockSIDX{
		storage: make(map[string][]mockElement),
		config:  config,
		stats: Stats{
			MemoryUsageBytes: 0,
			DiskUsageBytes:   0,
			ElementCount:     0,
			PartCount:        1, // Mock always has 1 "virtual" part
			QueryCount:       atomic.Int64{},
			WriteCount:       atomic.Int64{},
			LastFlushTime:    0,
			LastMergeTime:    0,
		},
	}
}

// Write performs batch write operations with in-memory storage.
// Elements are stored in a sorted slice by SeriesID, then by Key.
func (m *MockSIDX) Write(_ context.Context, reqs []WriteRequest) error {
	if m.closed.Load() {
		return fmt.Errorf("SIDX is closed")
	}

	if len(reqs) == 0 {
		return nil
	}

	// Simulate artificial delay if configured
	if m.config.WriteDelayMs > 0 {
		time.Sleep(time.Duration(m.config.WriteDelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if m.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < m.config.ErrorRate {
		return fmt.Errorf("mock error injection: write failed")
	}

	// Validate requests if strict validation is enabled
	if m.config.EnableStrictValidation {
		for i, req := range reqs {
			if err := req.Validate(); err != nil {
				return fmt.Errorf("invalid write request[%d]: %w", i, err)
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check element limit
	totalElements := m.getTotalElementsLocked()
	if m.config.MaxElements > 0 && totalElements+len(reqs) > m.config.MaxElements {
		return fmt.Errorf("element limit exceeded: current=%d, adding=%d, limit=%d",
			totalElements, len(reqs), m.config.MaxElements)
	}

	// Group requests by name (for proper batching)
	requestsByName := make(map[string][]WriteRequest)
	for _, req := range reqs {
		// Use SeriesID as name if not provided in context
		name := fmt.Sprintf("series_%d", req.SeriesID)
		requestsByName[name] = append(requestsByName[name], req)
	}

	// Process each name group
	for name, nameReqs := range requestsByName {
		elements := m.storage[name]

		// Convert requests to mock elements
		for _, req := range nameReqs {
			elem := mockElement{
				SeriesID: req.SeriesID,
				Key:      req.Key,
				Data:     make([]byte, len(req.Data)),
				Tags:     make([]tag, len(req.Tags)),
			}
			copy(elem.Data, req.Data)
			copy(elem.Tags, req.Tags)
			elements = append(elements, elem)
		}

		// Sort elements by SeriesID, then by Key
		sort.Slice(elements, func(i, j int) bool {
			if elements[i].SeriesID != elements[j].SeriesID {
				return elements[i].SeriesID < elements[j].SeriesID
			}
			return elements[i].Key < elements[j].Key
		})

		m.storage[name] = elements
		m.elementCounter.Add(int64(len(nameReqs)))
	}

	// Update stats
	m.stats.WriteCount.Add(1)
	m.stats.ElementCount = m.elementCounter.Load()
	m.updateMemoryUsageLocked()

	return nil
}

// Query executes a query with linear search and filtering.
func (m *MockSIDX) Query(_ context.Context, req QueryRequest) (QueryResult, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("SIDX is closed")
	}

	// Simulate artificial delay if configured
	if m.config.QueryDelayMs > 0 {
		time.Sleep(time.Duration(m.config.QueryDelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if m.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < m.config.ErrorRate {
		return nil, fmt.Errorf("mock error injection: query failed")
	}

	// Validate request if strict validation is enabled
	if m.config.EnableStrictValidation {
		if err := req.Validate(); err != nil {
			return nil, fmt.Errorf("invalid query request: %w", err)
		}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	elements, exists := m.storage[req.Name]
	if !exists {
		elements = []mockElement{}
	}

	// Update query stats
	m.stats.QueryCount.Add(1)

	// Create and return query result iterator
	return &mockSIDXQueryResult{
		elements: elements,
		request:  req,
		stats:    &m.stats,
	}, nil
}

// Stats returns current system statistics.
func (m *MockSIDX) Stats(_ context.Context) (*Stats, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("SIDX is closed")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a new stats struct to avoid copying atomic values
	stats := &Stats{
		MemoryUsageBytes: m.stats.MemoryUsageBytes,
		DiskUsageBytes:   m.stats.DiskUsageBytes,
		ElementCount:     m.elementCounter.Load(),
		PartCount:        m.stats.PartCount,
		LastFlushTime:    atomic.LoadInt64(&m.lastFlushTime),
		LastMergeTime:    atomic.LoadInt64(&m.lastMergeTime),
	}
	// Load atomic values
	stats.QueryCount.Store(m.stats.QueryCount.Load())
	stats.WriteCount.Store(m.stats.WriteCount.Load())
	return stats, nil
}

// Flush simulates persistence of memory parts to disk.
func (m *MockSIDX) Flush() error {
	if m.closed.Load() {
		return fmt.Errorf("SIDX is closed")
	}

	// Simulate artificial delay if configured
	if m.config.FlushDelayMs > 0 {
		time.Sleep(time.Duration(m.config.FlushDelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if m.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < m.config.ErrorRate {
		return fmt.Errorf("mock error injection: flush failed")
	}

	// Update flush time
	atomic.StoreInt64(&m.lastFlushTime, time.Now().UnixNano())

	return nil
}

// Merge simulates compaction of parts to optimize storage.
func (m *MockSIDX) Merge() error {
	if m.closed.Load() {
		return fmt.Errorf("SIDX is closed")
	}

	// Simulate artificial delay if configured
	if m.config.MergeDelayMs > 0 {
		time.Sleep(time.Duration(m.config.MergeDelayMs) * time.Millisecond)
	}

	// Simulate error injection if configured
	if m.config.ErrorRate > 0 && (int(time.Now().UnixNano())%100) < m.config.ErrorRate {
		return fmt.Errorf("mock error injection: merge failed")
	}

	// Update merge time
	atomic.StoreInt64(&m.lastMergeTime, time.Now().UnixNano())

	return nil
}

// Close gracefully shuts down the mock SIDX instance.
func (m *MockSIDX) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return fmt.Errorf("SIDX already closed")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear storage
	for name := range m.storage {
		delete(m.storage, name)
	}

	return nil
}

// getTotalElementsLocked returns the total number of elements across all series.
// Must be called with mu held.
func (m *MockSIDX) getTotalElementsLocked() int {
	total := 0
	for _, elements := range m.storage {
		total += len(elements)
	}
	return total
}

// updateMemoryUsageLocked updates the memory usage statistics.
// Must be called with mu held.
func (m *MockSIDX) updateMemoryUsageLocked() {
	var totalBytes int64
	for _, elements := range m.storage {
		for _, elem := range elements {
			totalBytes += int64(len(elem.Data))
			for _, tag := range elem.Tags {
				totalBytes += int64(len(tag.value))
				totalBytes += int64(len(tag.name))
			}
		}
	}
	m.stats.MemoryUsageBytes = totalBytes
}

// mockSIDXQueryResult implements QueryResult for the mock implementation.
type mockSIDXQueryResult struct {
	stats    *Stats
	elements []mockElement
	request  QueryRequest
	position int
	finished bool
}

// Pull returns the next batch of query results.
func (mqr *mockSIDXQueryResult) Pull() *QueryResponse {
	if mqr.finished || mqr.position >= len(mqr.elements) {
		return nil
	}

	startTime := time.Now()
	response := &QueryResponse{}

	// Filter elements based on query parameters
	var matches []mockElement
	elementsScanned := 0
	elementsFiltered := 0

	for i := mqr.position; i < len(mqr.elements); i++ {
		elem := mqr.elements[i]
		elementsScanned++

		// Apply filtering logic
		if mqr.matchesFilter(elem) {
			matches = append(matches, elem)
			elementsFiltered++

			// Limit batch size
			if mqr.request.MaxElementSize > 0 && len(matches) >= mqr.request.MaxElementSize {
				mqr.position = i + 1
				break
			}
		}

		// Update position for next iteration
		mqr.position = i + 1
	}

	// If we've processed all elements, mark as finished
	if mqr.position >= len(mqr.elements) {
		mqr.finished = true
	}

	// Convert matches to response format
	response.Keys = make([]int64, len(matches))
	response.Data = make([][]byte, len(matches))
	response.Tags = make([][]tag, len(matches))
	response.SIDs = make([]common.SeriesID, len(matches))

	for i, match := range matches {
		response.Keys[i] = match.Key
		response.Data[i] = make([]byte, len(match.Data))
		copy(response.Data[i], match.Data)
		response.Tags[i] = make([]tag, len(match.Tags))
		copy(response.Tags[i], match.Tags)
		response.SIDs[i] = match.SeriesID
	}

	// Fill metadata
	response.Metadata = ResponseMetadata{
		ExecutionTimeMs:  time.Since(startTime).Milliseconds(),
		ElementsScanned:  int64(elementsScanned),
		ElementsFiltered: int64(elementsFiltered),
		PartsAccessed:    1,   // Mock has 1 virtual part
		BlocksScanned:    1,   // Mock scans 1 virtual block
		CacheHitRatio:    1.0, // Everything is in "memory cache"
		TruncatedResults: mqr.request.MaxElementSize > 0 && len(matches) >= mqr.request.MaxElementSize,
	}

	return response
}

// Release releases resources associated with the query result.
func (mqr *mockSIDXQueryResult) Release() {
	mqr.elements = nil
	mqr.finished = true
}

// matchesFilter applies basic filtering logic to determine if an element matches the query.
func (mqr *mockSIDXQueryResult) matchesFilter(elem mockElement) bool {
	// Apply entity filtering if specified
	// In a real implementation this would be more sophisticated
	// For now, we'll skip entity filtering in the mock

	// Apply tag filtering using Filter interface
	if mqr.request.Filter != nil {
		// Convert element tags to a format suitable for filter evaluation
		// This is a simplified implementation
		for _, tag := range elem.Tags {
			// For the mock, we'll do basic tag matching
			// In practice, this would use proper filter evaluation
			_ = tag // Simplified filtering for mock
		}
	}

	return true
}

// SetErrorRate allows dynamic configuration of error injection rate during testing.
func (m *MockSIDX) SetErrorRate(rate int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rate >= 0 && rate <= 100 {
		m.config.ErrorRate = rate
	}
}

// SetWriteDelay allows dynamic configuration of write delay during testing.
func (m *MockSIDX) SetWriteDelay(delayMs int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if delayMs >= 0 {
		m.config.WriteDelayMs = delayMs
	}
}

// SetQueryDelay allows dynamic configuration of query delay during testing.
func (m *MockSIDX) SetQueryDelay(delayMs int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if delayMs >= 0 {
		m.config.QueryDelayMs = delayMs
	}
}

// GetElementCount returns the current number of elements stored (for testing).
func (m *MockSIDX) GetElementCount() int64 {
	return m.elementCounter.Load()
}

// GetStorageKeys returns all storage keys (for testing and debugging).
func (m *MockSIDX) GetStorageKeys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.storage))
	for key := range m.storage {
		keys = append(keys, key)
	}
	return keys
}

// Clear removes all stored elements (for testing).
func (m *MockSIDX) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name := range m.storage {
		delete(m.storage, name)
	}
	m.elementCounter.Store(0)
	m.stats.ElementCount = 0
	m.updateMemoryUsageLocked()
}
