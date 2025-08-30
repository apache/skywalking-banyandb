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
	"sync"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
)

// QueryMultipleSIDX executes the same query across multiple SIDX instances in parallel.
// and merges their results maintaining the specified ordering.
// This function is useful for distributed scenarios where data is partitioned across
// multiple SIDX instances and a unified view is needed.
//
// Parameters:
// - ctx: Context for cancellation and timeout control
// - sidxs: Slice of SIDX instances to query
// - req: QueryRequest to execute on all SIDX instances
//
// Returns:
// - *QueryResponse: Merged response from all SIDX instances with maintained ordering
// - error: Aggregated errors from failed SIDX queries, or nil if at least one succeeds
//
// Behavior:
// - Queries are executed in parallel across all SIDX instances
// - Results are merged maintaining ASC/DESC order as specified in QueryRequest
// - MaxElementSize limits are respected across the merged result
// - Partial failures are tolerated - returns success if at least one SIDX succeeds
// - All errors are aggregated and returned if no SIDX instances succeed
func QueryMultipleSIDX(ctx context.Context, sidxs []SIDX, req QueryRequest) (*QueryResponse, error) {
	if len(sidxs) == 0 {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	// Validate the request once before distributing to all SIDX instances
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid query request: %w", err)
	}

	// Single SIDX optimization
	if len(sidxs) == 1 {
		return sidxs[0].Query(ctx, req)
	}

	// Parallel execution setup
	type sidxResult struct {
		response *QueryResponse
		err      error
		index    int
	}

	resultCh := make(chan sidxResult, len(sidxs))
	var wg sync.WaitGroup

	// Launch queries in parallel
	for i, sidxInstance := range sidxs {
		wg.Add(1)
		go func(idx int, sidx SIDX) {
			defer wg.Done()
			resp, err := sidx.Query(ctx, req)
			resultCh <- sidxResult{
				response: resp,
				err:      err,
				index:    idx,
			}
		}(i, sidxInstance)
	}

	// Wait for all queries to complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results
	var successfulResponses []*QueryResponse
	var aggregatedError error

	for result := range resultCh {
		if result.err != nil {
			aggregatedError = multierr.Append(aggregatedError,
				fmt.Errorf("SIDX[%d] query failed: %w", result.index, result.err))
		} else if result.response != nil {
			successfulResponses = append(successfulResponses, result.response)
		}
	}

	// Return error if no successful responses
	if len(successfulResponses) == 0 {
		if aggregatedError != nil {
			return nil, fmt.Errorf("all SIDX queries failed: %w", aggregatedError)
		}
		// All succeeded but returned nil responses (shouldn't happen but handle gracefully)
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	// Single successful response optimization
	if len(successfulResponses) == 1 {
		response := successfulResponses[0]
		// Include partial failure information if there were errors
		if aggregatedError != nil {
			response.Error = aggregatedError
		}
		return response, nil
	}

	// Merge multiple responses
	mergedResponse := mergeMultipleSIDXResponses(successfulResponses, req)
	
	// Include partial failure information in the response if there were errors
	if aggregatedError != nil {
		mergedResponse.Error = aggregatedError
	}

	return mergedResponse, nil
}

// mergeMultipleSIDXResponses merges multiple QueryResponse instances maintaining order.
// This function leverages the existing merge logic but adapts it for multi-SIDX usage.
func mergeMultipleSIDXResponses(responses []*QueryResponse, req QueryRequest) *QueryResponse {
	if len(responses) == 0 {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}
	}

	if len(responses) == 1 {
		return responses[0]
	}

	// Determine ordering from request
	asc := true // Default ascending
	if req.Order != nil {
		asc = extractOrdering(req)
	}

	// Use existing merge functions
	if asc {
		return mergeQueryResponseShardsAsc(responses, req.MaxElementSize)
	}
	return mergeQueryResponseShardsDesc(responses, req.MaxElementSize)
}

// QueryMultipleSIDXWithOptions provides additional configuration options for multi-SIDX queries.
// This is an extended version of QueryMultipleSIDX with more control over execution behavior.
type MultiSIDXQueryOptions struct {
	// FailFast determines whether to return immediately on first error or collect all results
	FailFast bool
	
	// MinSuccessCount specifies minimum number of successful SIDX queries required
	// If less than MinSuccessCount succeed, the function returns an error
	MinSuccessCount int
}

// QueryMultipleSIDXWithOptions executes queries with additional configuration options.
func QueryMultipleSIDXWithOptions(ctx context.Context, sidxs []SIDX, req QueryRequest, opts MultiSIDXQueryOptions) (*QueryResponse, error) {
	if opts.MinSuccessCount <= 0 {
		opts.MinSuccessCount = 1 // Default: at least one success required
	}

	if len(sidxs) == 0 {
		if opts.MinSuccessCount > 0 {
			return nil, fmt.Errorf("no SIDX instances provided, cannot meet minimum success count of %d", opts.MinSuccessCount)
		}
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	// Validate the request once before distributing
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid query request: %w", err)
	}

	// Use regular function for simple cases
	if !opts.FailFast && opts.MinSuccessCount == 1 {
		return QueryMultipleSIDX(ctx, sidxs, req)
	}

	// Extended implementation with options
	type sidxResult struct {
		response *QueryResponse
		err      error
		index    int
	}

	resultCh := make(chan sidxResult, len(sidxs))
	var wg sync.WaitGroup

	// Context for early termination if FailFast is enabled
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Launch queries in parallel
	for i, sidxInstance := range sidxs {
		wg.Add(1)
		go func(idx int, sidx SIDX) {
			defer wg.Done()
			resp, err := sidx.Query(queryCtx, req)
			
			select {
			case resultCh <- sidxResult{
				response: resp,
				err:      err,
				index:    idx,
			}:
			case <-queryCtx.Done():
				// Context canceled, don't send result
			}
		}(i, sidxInstance)
	}

	// Wait for all queries to complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results with options
	var successfulResponses []*QueryResponse
	var aggregatedError error
	successCount := 0
	failureCount := 0

	for result := range resultCh {
		if result.err != nil {
			failureCount++
			aggregatedError = multierr.Append(aggregatedError,
				fmt.Errorf("SIDX[%d] query failed: %w", result.index, result.err))
			
			// Fail fast on first error if enabled
			if opts.FailFast {
				cancel() // Cancel remaining queries
				return nil, fmt.Errorf("query failed fast at SIDX[%d]: %w", result.index, result.err)
			}
		} else if result.response != nil {
			successCount++
			successfulResponses = append(successfulResponses, result.response)
		}
	}

	// Check minimum success count
	if successCount < opts.MinSuccessCount {
		return nil, fmt.Errorf("insufficient successful queries: got %d, required %d (failures: %d): %w",
			successCount, opts.MinSuccessCount, failureCount, aggregatedError)
	}

	// Return merged results
	if len(successfulResponses) == 0 {
		return &QueryResponse{
			Keys: make([]int64, 0),
			Data: make([][]byte, 0),
			Tags: make([][]Tag, 0),
			SIDs: make([]common.SeriesID, 0),
		}, nil
	}

	mergedResponse := mergeMultipleSIDXResponses(successfulResponses, req)
	
	// Include partial failure information if there were errors
	if aggregatedError != nil && !opts.FailFast {
		mergedResponse.Error = aggregatedError
	}

	return mergedResponse, nil
}