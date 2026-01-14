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

package common

import (
	"context"
	"sync"
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// NodeFetcher defines the interface for fetching node metadata.
type NodeFetcher interface {
	// FetchNodeWithRetry attempts to fetch node metadata for the given address.
	FetchNodeWithRetry(ctx context.Context, address string) (*databasev1.Node, error)
}

// RetryMetrics defines metrics interface for retry operations.
type RetryMetrics interface {
	IncRetryCount()
	IncRetrySuccess()
	IncRetryFailed()
	SetQueueSize(size float64)
}

// RetryConfig holds retry backoff configuration.
type RetryConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// RetryState tracks backoff retry state for a single node.
type RetryState struct {
	nextRetryTime  time.Time
	lastError      error
	address        string
	attemptCount   int
	currentBackoff time.Duration
}

// RetryManager manages retry queue and scheduling for failed node fetches.
type RetryManager struct {
	fetcher       NodeFetcher
	metrics       RetryMetrics
	cacheBase     *NodeCacheBase
	retryQueue    map[string]*RetryState
	log           *logger.Logger
	onSuccess     func(address string, node *databasev1.Node)
	onAddedToNode func(address string)
	config        RetryConfig
	retryMutex    sync.RWMutex
}

// NewRetryManager creates a new retry manager.
func NewRetryManager(
	fetcher NodeFetcher,
	cacheBase *NodeCacheBase,
	config RetryConfig,
	metrics RetryMetrics,
) *RetryManager {
	return &RetryManager{
		fetcher:    fetcher,
		cacheBase:  cacheBase,
		retryQueue: make(map[string]*RetryState),
		config:     config,
		metrics:    metrics,
		log:        cacheBase.GetLogger(),
	}
}

// SetSuccessCallback sets the callback to be called when a node is successfully fetched after retry.
func (r *RetryManager) SetSuccessCallback(callback func(address string, node *databasev1.Node)) {
	r.onSuccess = callback
}

// SetAddedToNodeCallback sets the callback to be called when a node is added to cache.
func (r *RetryManager) SetAddedToNodeCallback(callback func(address string)) {
	r.onAddedToNode = callback
}

// SetMetrics sets the metrics for the retry manager.
func (r *RetryManager) SetMetrics(metrics RetryMetrics) {
	r.metrics = metrics
}

// AddToRetry adds a failed node fetch to the retry queue.
func (r *RetryManager) AddToRetry(address string, fetchErr error) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()

	r.retryQueue[address] = &RetryState{
		address:        address,
		attemptCount:   1,
		nextRetryTime:  time.Now().Add(r.config.InitialInterval),
		currentBackoff: r.config.InitialInterval,
		lastError:      fetchErr,
	}

	if r.metrics != nil {
		r.metrics.IncRetryCount()
		r.metrics.SetQueueSize(float64(len(r.retryQueue)))
	}

	r.log.Warn().
		Err(fetchErr).
		Str("address", address).
		Msg("Failed to fetch node metadata, adding to retry queue")
}

// RemoveFromRetry removes an address from the retry queue.
func (r *RetryManager) RemoveFromRetry(address string) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()

	delete(r.retryQueue, address)
	if r.metrics != nil {
		r.metrics.SetQueueSize(float64(len(r.retryQueue)))
	}

	r.log.Debug().
		Str("address", address).
		Msg("Removed node from retry queue")
}

// IsInRetry checks if an address is in the retry queue.
func (r *RetryManager) IsInRetry(address string) bool {
	r.retryMutex.RLock()
	defer r.retryMutex.RUnlock()
	_, exists := r.retryQueue[address]
	return exists
}

// GetQueueSize returns the current retry queue size.
func (r *RetryManager) GetQueueSize() int {
	r.retryMutex.RLock()
	defer r.retryMutex.RUnlock()
	return len(r.retryQueue)
}

// ProcessRetryQueue processes nodes that are ready for retry.
func (r *RetryManager) ProcessRetryQueue(ctx context.Context) {
	now := time.Now()

	r.retryMutex.RLock()
	nodesToRetry := make([]*RetryState, 0)
	for _, retryState := range r.retryQueue {
		if retryState.nextRetryTime.Before(now) || retryState.nextRetryTime.Equal(now) {
			nodesToRetry = append(nodesToRetry, retryState)
		}
	}
	r.retryMutex.RUnlock()

	if len(nodesToRetry) == 0 {
		return
	}

	r.log.Debug().Int("count", len(nodesToRetry)).Msg("Processing retry queue")

	for _, retryState := range nodesToRetry {
		r.attemptNodeRetry(ctx, retryState)
	}
}

func (r *RetryManager) attemptNodeRetry(ctx context.Context, retryState *RetryState) {
	addr := retryState.address

	r.log.Debug().
		Str("address", addr).
		Int("attempt", retryState.attemptCount).
		Dur("backoff", retryState.currentBackoff).
		Msg("Attempting node metdata fetch retry")

	// try to fetch metadata
	node, fetchErr := r.fetcher.FetchNodeWithRetry(ctx, addr)

	if fetchErr != nil {
		// retry failed - update backoff
		retryState.attemptCount++
		retryState.lastError = fetchErr

		// calculate next backoff interval
		nextBackoff := time.Duration(float64(retryState.currentBackoff) * r.config.Multiplier)
		if nextBackoff > r.config.MaxInterval {
			nextBackoff = r.config.MaxInterval
		}
		retryState.currentBackoff = nextBackoff
		retryState.nextRetryTime = time.Now().Add(nextBackoff)

		r.log.Warn().
			Err(fetchErr).
			Str("address", addr).
			Int("attempt", retryState.attemptCount).
			Dur("next_backoff", nextBackoff).
			Time("next_retry", retryState.nextRetryTime).
			Msg("Node metadata fetch retry failed, scheduling next retry")

		r.retryMutex.Lock()
		r.retryQueue[addr] = retryState
		r.retryMutex.Unlock()

		if r.metrics != nil {
			r.metrics.IncRetryFailed()
		}
		return
	}

	// success - add to cache
	r.cacheBase.AddNode(addr, node)

	// remove from retry queue
	r.retryMutex.Lock()
	delete(r.retryQueue, addr)
	queueSize := len(r.retryQueue)
	r.retryMutex.Unlock()

	// notify handlers
	r.cacheBase.NotifyHandlers(schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind: schema.KindNode,
			Name: node.GetMetadata().GetName(),
		},
		Spec: node,
	}, true)

	r.log.Info().
		Str("address", addr).
		Str("name", node.GetMetadata().GetName()).
		Int("total_attempts", retryState.attemptCount).
		Msg("Node metadata fetched successfully after retry")

	// update metrics
	if r.metrics != nil {
		r.metrics.IncRetrySuccess()
		r.metrics.SetQueueSize(float64(queueSize))
	}

	// call success callback if set
	if r.onSuccess != nil {
		r.onSuccess(addr, node)
	}

	// call added to node callback if set
	if r.onAddedToNode != nil {
		r.onAddedToNode(addr)
	}
}

// CleanupRetryQueue removes addresses from retry queue that are not in the provided set.
func (r *RetryManager) CleanupRetryQueue(validAddresses map[string]bool) {
	r.retryMutex.Lock()
	defer r.retryMutex.Unlock()

	for addr := range r.retryQueue {
		if !validAddresses[addr] {
			delete(r.retryQueue, addr)
			r.log.Debug().
				Str("address", addr).
				Msg("Removed node from retry queue (no longer valid)")
		}
	}

	if r.metrics != nil {
		r.metrics.SetQueueSize(float64(len(r.retryQueue)))
	}
}
