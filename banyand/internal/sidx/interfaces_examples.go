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

// Package sidx provides comprehensive interface usage examples and integration patterns
// for the Secondary Index File System (SIDX). This file demonstrates proper usage
// of SIDX interfaces, best practices, and integration with BanyanDB core storage.
package sidx

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// InterfaceUsageExamples demonstrates comprehensive usage patterns for SIDX interfaces.
// These examples show real-world scenarios and proper error handling patterns.
type InterfaceUsageExamples struct {
	sidx SIDX
}

// NewInterfaceUsageExamples creates a new example instance with a mock SIDX implementation.
// In production, this would be replaced with an actual SIDX instance.
func NewInterfaceUsageExamples() *InterfaceUsageExamples {
	// Configuration setup - demonstrates proper Options usage with mandatory parameters
	memory := protector.NewMemory(observability.NewBypassRegistry())
	opts, err := NewOptions("/data/sidx", memory)
	if err != nil {
		// In production, handle this error appropriately
		panic(fmt.Sprintf("failed to create options: %v", err))
	}

	// Further customize options if needed
	_ = opts.WithMergePolicy(NewMergePolicy(8, 1.7, 1<<30))

	// In production, this would create an actual SIDX instance:
	// sidx, err := NewSIDX(ctx, opts)
	// For examples, we use a mock implementation
	sidx := &mockSIDX{}

	return &InterfaceUsageExamples{
		sidx: sidx,
	}
}

// BasicWriteExample demonstrates basic batch writing with proper error handling.
// This shows the fundamental write pattern used throughout BanyanDB.
func (e *InterfaceUsageExamples) BasicWriteExample(ctx context.Context) error {
	// Prepare batch write requests - demonstrates proper WriteRequest usage
	writeReqs := []WriteRequest{
		{
			SeriesID: common.SeriesID(1001),
			Key:      1640995200000, // Unix timestamp in milliseconds (opaque to SIDX)
			Data:     []byte(`{"service": "user-service", "endpoint": "/api/users"}`),
			Tags: []Tag{
				{name: "service", value: []byte("user-service")},
				{name: "endpoint", value: []byte("/api/users")},
				{name: "status_code", value: int64ToBytes(200)},
			},
		},
		{
			SeriesID: common.SeriesID(1001),
			Key:      1640995260000,
			Data:     []byte(`{"service": "user-service", "endpoint": "/api/login"}`),
			Tags: []Tag{
				{name: "service", value: []byte("user-service")},
				{name: "endpoint", value: []byte("/api/login")},
				{name: "status_code", value: int64ToBytes(401)},
			},
		},
	}

	// Execute batch write with timeout context
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := e.sidx.Write(writeCtx, writeReqs); err != nil {
		return fmt.Errorf("batch write failed: %w", err)
	}

	log.Printf("Successfully wrote %d elements to SIDX", len(writeReqs))
	return nil
}

// AdvancedQueryExample demonstrates complex querying with filtering and projection.
// Shows integration with BanyanDB's index.Filter and query patterns.
func (e *InterfaceUsageExamples) AdvancedQueryExample(ctx context.Context) error {
	// Create query request with range and tag filtering
	queryReq := QueryRequest{
		Name: "trace-sidx",
		Entities: [][]*modelv1.TagValue{
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "user-service"}}},
			},
		},
		Filter: nil, // In production, use actual index.Filter implementation
		Order:  nil, // In production, use actual index.OrderBy implementation
		TagProjection: []model.TagProjection{
			{Family: "service", Names: []string{"name"}},
			{Family: "endpoint", Names: []string{"path"}},
			{Family: "status", Names: []string{"code"}},
		},
		MaxElementSize: 1000,
	}

	// Execute query with timeout context
	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	result, err := e.sidx.Query(queryCtx, queryReq)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer result.Release() // Critical: always release resources

	// Iterate through results using Pull pattern
	totalResults := 0
	for {
		response := result.Pull()
		if response == nil {
			break // No more results
		}

		// Check for execution errors during iteration
		if response.Error != nil {
			return fmt.Errorf("query execution error: %w", response.Error)
		}

		// Process results batch
		for i := 0; i < response.Len(); i++ {
			log.Printf("Result %d: Key=%d, SeriesID=%d, Data=%s",
				totalResults+i+1,
				response.Keys[i],
				response.SIDs[i],
				string(response.Data[i]))

			// Process tags if needed
			if i < len(response.Tags) {
				for _, tag := range response.Tags[i] {
					log.Printf("  Tag: %s = %s", tag.name, string(tag.value))
				}
			}
		}

		totalResults += response.Len()
		log.Printf("Processed batch: %d results, metadata: %+v",
			response.Len(), response.Metadata)
	}

	log.Printf("Query completed: %d total results", totalResults)
	return nil
}

// FlushAndMergeExample demonstrates manual control over persistence and compaction.
// Shows when and how to trigger flush and merge operations for optimal performance.
func (e *InterfaceUsageExamples) FlushAndMergeExample(ctx context.Context) error {
	// Check stats before operations
	stats, err := e.sidx.Stats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get stats: %w", err)
	}
	if stats == nil {
		return fmt.Errorf("stats is nil")
	}

	log.Printf("Before operations - Memory: %d bytes, Disk: %d bytes, Parts: %d",
		stats.MemoryUsageBytes, stats.DiskUsageBytes, stats.PartCount)

	// Trigger flush to persist memory parts
	if flushErr := e.sidx.Flush(); flushErr != nil {
		return fmt.Errorf("flush operation failed: %w", flushErr)
	}
	log.Println("Flush completed successfully")

	// Trigger merge to compact parts
	if mergeErr := e.sidx.Merge(); mergeErr != nil {
		return fmt.Errorf("merge operation failed: %w", mergeErr)
	}
	log.Println("Merge completed successfully")

	// Check stats after operations
	statsAfter, err := e.sidx.Stats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get stats after operations: %w", err)
	}
	if statsAfter == nil {
		return fmt.Errorf("stats after operations is nil")
	}

	log.Printf("After operations - Memory: %d bytes, Disk: %d bytes, Parts: %d",
		statsAfter.MemoryUsageBytes, statsAfter.DiskUsageBytes, statsAfter.PartCount)

	return nil
}

// ErrorHandlingExample demonstrates comprehensive error handling patterns.
// Shows how to handle different types of errors and recovery strategies.
func (e *InterfaceUsageExamples) ErrorHandlingExample(ctx context.Context) {
	// Example 1: Write error handling
	writeReqs := []WriteRequest{
		{
			SeriesID: common.SeriesID(0), // Invalid SeriesID
			Key:      1640995200000,
			Data:     []byte("test data"),
			Tags:     []Tag{{name: "test", value: []byte("value")}},
		},
	}

	if err := e.sidx.Write(ctx, writeReqs); err != nil {
		log.Printf("Write error (expected): %v", err)
		// In production: implement retry logic, circuit breaker, etc.
	}

	// Example 2: Query error handling
	invalidQuery := QueryRequest{
		Name:           "", // Invalid name
		MaxElementSize: -1, // Invalid size
	}

	result, err := e.sidx.Query(ctx, invalidQuery)
	if err != nil {
		log.Printf("Query setup error (expected): %v", err)
		// Handle setup errors - usually validation failures
		return
	}

	if result != nil {
		defer result.Release()

		// Handle execution errors during iteration
		for {
			response := result.Pull()
			if response == nil {
				break
			}

			if response.Error != nil {
				log.Printf("Query execution error: %v", response.Error)
				// Handle execution errors - partial results may be available
				break
			}
		}
	}
}

// PerformanceOptimizationExample demonstrates best practices for optimal performance.
// Shows batching, sorting, and resource management strategies.
func (e *InterfaceUsageExamples) PerformanceOptimizationExample(ctx context.Context) error {
	// Best Practice 1: Batch writes for optimal throughput
	const batchSize = 1000
	writeReqs := make([]WriteRequest, 0, batchSize)

	// Best Practice 2: Pre-sort elements by SeriesID then Key
	// This optimizes block construction and reduces fragmentation
	for seriesID := 1; seriesID <= 10; seriesID++ {
		for i := 0; i < 100; i++ {
			writeReqs = append(writeReqs, WriteRequest{
				SeriesID: common.SeriesID(seriesID),
				Key:      int64(1640995200000 + i*1000), // Sequential keys
				Data:     []byte(fmt.Sprintf(`{"series":%d,"seq":%d}`, seriesID, i)),
				Tags: []Tag{
					{name: "series_id", value: int64ToBytes(int64(seriesID))},
					{name: "sequence", value: int64ToBytes(int64(i))},
				},
			})
		}
	}

	// Execute optimized batch write
	start := time.Now()
	if err := e.sidx.Write(ctx, writeReqs); err != nil {
		return fmt.Errorf("optimized batch write failed: %w", err)
	}
	writeDuration := time.Since(start)

	log.Printf("Optimized batch write: %d elements in %v (%.2f elem/sec)",
		len(writeReqs), writeDuration, float64(len(writeReqs))/writeDuration.Seconds())

	// Best Practice 3: Use appropriate query limits
	queryReq := QueryRequest{
		Name:           "performance-test",
		MaxElementSize: 100, // Reasonable batch size
		TagProjection: []model.TagProjection{
			{Family: "series", Names: []string{"id"}}, // Only project needed tags
		},
	}

	result, err := e.sidx.Query(ctx, queryReq)
	if err != nil {
		return fmt.Errorf("performance query failed: %w", err)
	}
	defer result.Release()

	// Process results efficiently
	queryStart := time.Now()
	resultCount := 0
	for {
		response := result.Pull()
		if response == nil {
			break
		}
		if response.Error != nil {
			return fmt.Errorf("query execution error: %w", response.Error)
		}
		resultCount += response.Len()
	}
	queryDuration := time.Since(queryStart)

	log.Printf("Optimized query: %d results in %v (%.2f results/sec)",
		resultCount, queryDuration, float64(resultCount)/queryDuration.Seconds())

	return nil
}

// IntegrationPatternExample demonstrates integration with BanyanDB core storage.
// Shows how SIDX interfaces integrate with existing BanyanDB patterns and workflows.
func (e *InterfaceUsageExamples) IntegrationPatternExample(ctx context.Context) error {
	// Integration Pattern 1: Following BanyanDB's context-aware operations
	// Use context for timeout control and cancellation
	operationCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Integration Pattern 2: Statistics monitoring (like other BanyanDB components)
	stats, err := e.sidx.Stats(operationCtx)
	if err != nil {
		return fmt.Errorf("stats integration failed: %w", err)
	}
	if stats == nil {
		return fmt.Errorf("stats is nil")
	}

	// Integration Pattern 3: Metric collection for observability
	log.Printf("SIDX Metrics - Elements: %d, Parts: %d, Queries: %d",
		stats.ElementCount, stats.PartCount, stats.QueryCount.Load())

	// Integration Pattern 4: Resource management following BanyanDB patterns
	// Proper cleanup and resource release
	defer func() {
		if closeErr := e.sidx.Close(); closeErr != nil {
			log.Printf("SIDX close error: %v", closeErr)
		}
	}()

	// Integration Pattern 5: Error handling consistent with BanyanDB
	// Use structured errors and proper error wrapping
	if stats.MemoryUsageBytes > 1<<30 { // 1GB threshold
		log.Println("High memory usage detected, triggering maintenance")

		if err := e.sidx.Flush(); err != nil {
			// Don't fail the operation, but log the issue
			log.Printf("Maintenance flush failed: %v", err)
		}

		if err := e.sidx.Merge(); err != nil {
			log.Printf("Maintenance merge failed: %v", err)
		}
	}

	return nil
}

// int64ToBytes converts an int64 to bytes for tag values.
// This utility function demonstrates proper tag value encoding.
func int64ToBytes(val int64) []byte {
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		result[7-i] = byte(val >> (8 * i))
	}
	return result
}

// mockSIDX provides a simple mock implementation for example purposes.
// In production, this would be replaced with the actual SIDX implementation.
type mockSIDX struct{}

func (m *mockSIDX) Write(_ context.Context, reqs []WriteRequest) error {
	if len(reqs) == 0 {
		return fmt.Errorf("empty write request")
	}
	for i, req := range reqs {
		if req.SeriesID == 0 {
			return fmt.Errorf("invalid SeriesID at index %d", i)
		}
	}
	return nil
}

func (m *mockSIDX) Query(_ context.Context, req QueryRequest) (QueryResult, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("query name cannot be empty")
	}
	if req.MaxElementSize < 0 {
		return nil, fmt.Errorf("invalid MaxElementSize: %d", req.MaxElementSize)
	}
	return &mockQueryResult{}, nil
}

func (m *mockSIDX) Stats(_ context.Context) (*Stats, error) {
	stats := &Stats{
		MemoryUsageBytes: 1024 * 1024 * 100, // 100MB
		DiskUsageBytes:   1024 * 1024 * 500, // 500MB
		ElementCount:     10000,
		PartCount:        5,
	}
	stats.QueryCount.Store(1000)
	stats.WriteCount.Store(0)
	return stats, nil
}

func (m *mockSIDX) Close() error {
	return nil
}

func (m *mockSIDX) Flush() error {
	return nil
}

func (m *mockSIDX) Merge() error {
	return nil
}

// mockQueryResult provides a simple mock query result for examples.
type mockQueryResult struct {
	pulled bool
}

func (m *mockQueryResult) Pull() *QueryResponse {
	if m.pulled {
		return nil // No more results
	}
	m.pulled = true

	return &QueryResponse{
		Keys: []int64{1640995200000, 1640995260000},
		Data: [][]byte{
			[]byte(`{"service": "user-service", "endpoint": "/api/users"}`),
			[]byte(`{"service": "user-service", "endpoint": "/api/login"}`),
		},
		Tags: [][]Tag{
			{{name: "service", value: []byte("user-service")}},
			{{name: "service", value: []byte("user-service")}},
		},
		SIDs: []common.SeriesID{1001, 1001},
		Metadata: ResponseMetadata{
			ExecutionTimeMs:  100,
			ElementsScanned:  1000,
			ElementsFiltered: 998,
		},
	}
}

func (m *mockQueryResult) Release() {
	// Mock implementation - no resources to release
}

// Note: In production code, you would use actual index.Filter and index.OrderBy implementations
// from the BanyanDB index package. These examples use nil values for simplicity,
// but real applications should create proper filter and ordering specifications.

// Contract Specifications and Testing Guidelines
//
// The following section provides contract specifications for each interface
// to guide testing and implementation verification.

// Contract defines the behavioral contract for SIDX implementations.
// All SIDX implementations must satisfy these contracts.
type Contract struct {
	// Write Contract:
	// - MUST accept batch writes with pre-sorted elements
	// - MUST validate WriteRequest fields (non-zero SeriesID, valid tags)
	// - MUST handle context cancellation gracefully
	// - MUST return detailed error information for validation failures
	// - SHOULD optimize for sequential key writes within series

	// Query Contract:
	// - MUST return QueryResult that implements Pull/Release pattern
	// - MUST handle context timeout and cancellation
	// - MUST validate QueryRequest parameters before execution
	// - MUST provide error information in QueryResponse.Error for execution failures
	// - MUST maintain result ordering based on QueryRequest.Order
	// - MUST respect MaxElementSize limits

	// Stats Contract:
	// - MUST return current system metrics
	// - MUST handle context timeout
	// - SHOULD provide accurate memory and disk usage information
	// - SHOULD update counters atomically

	// Close Contract:
	// - MUST ensure all data is persisted before closing
	// - MUST release all resources
	// - MUST be idempotent (safe to call multiple times)
	// - SHOULD complete within reasonable time (suggest timeout)

	// Flush Contract:
	// - MUST persist memory parts to disk
	// - MUST be synchronous operation
	// - MUST coordinate with snapshot management
	// - SHOULD optimize part selection for efficiency

	// Merge Contract:
	// - MUST maintain key ordering during merge
	// - MUST be synchronous operation
	// - MUST coordinate with snapshot management
	// - SHOULD optimize part selection to minimize write amplification
}

// Performance Considerations and Best Practices
//
// 1. **Write Performance**:
//    - Always use batch writes instead of individual writes
//    - Pre-sort elements by SeriesID then Key for optimal block construction
//    - Use sequential keys within series when possible
//    - Monitor memory usage and trigger flushes proactively
//
// 2. **Query Performance**:
//    - Use appropriate MaxElementSize to balance memory and latency
//    - Project only needed tags to reduce I/O
//    - Use key range filters to minimize part scanning
//    - Release QueryResult promptly to free resources
//
// 3. **Resource Management**:
//    - Monitor Stats regularly for system health
//    - Trigger Flush operations before memory limits
//    - Use Merge operations to control part count and storage efficiency
//    - Always call Close() for clean shutdown
//
// 4. **Error Handling**:
//    - Distinguish between setup errors (Query return) and execution errors (QueryResponse.Error)
//    - Implement retry logic for transient failures
//    - Use context timeout for all operations
//    - Log detailed error information for debugging
//
// 5. **Integration Patterns**:
//    - Follow BanyanDB context patterns for timeout and cancellation
//    - Use structured logging for observability
//    - Implement proper metrics collection
//    - Follow BanyanDB error handling conventions
