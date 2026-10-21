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

package vectorized

import (
	"context"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

type queryMemoryScopeKey struct{}

type queryMemoryScope struct {
	tracker  *MemoryTracker
	encoding *responseEncodingRun
	mu       sync.Mutex
}

func (s *queryMemoryScope) initialize(ctx context.Context, limit int64) *MemoryTracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tracker == nil {
		s.tracker = newQueryMemoryTracker(ctx, limit)
	}
	return s.tracker
}

// WithQueryMemoryScope shares a lazily configured tracker with downstream execution.
// The processor supplies the actual memory limit when it initializes the tracker;
// the caller can subsequently account final encoding against that same allowance.
func WithQueryMemoryScope(ctx context.Context) context.Context {
	if _, ok := ctx.Value(queryMemoryScopeKey{}).(*queryMemoryScope); ok {
		return ctx
	}
	return context.WithValue(ctx, queryMemoryScopeKey{}, &queryMemoryScope{})
}

type responseEncodingRunKey struct{}

type responseEncodingRun struct {
	scope    *queryMemoryScope
	batches  [][]*measurev1.InternalDataPoint
	bound    uint64
	rows     uint64
	covered  bool
	complete bool
}

// BeginResponseEncoding starts coverage for one final serialization. A new run
// replaces any unfinished or unconsumed predecessor in the same query scope.
func BeginResponseEncoding(ctx context.Context) context.Context {
	scope, ok := ctx.Value(queryMemoryScopeKey{}).(*queryMemoryScope)
	if !ok {
		return ctx
	}
	run := &responseEncodingRun{scope: scope, covered: true}
	scope.mu.Lock()
	if previous := scope.encoding; previous != nil {
		previous.covered = false
		previous.complete = false
		previous.batches = nil
	}
	scope.encoding = run
	scope.mu.Unlock()
	return context.WithValue(ctx, responseEncodingRunKey{}, run)
}

// HasResponseEncoding reports whether final serialization retains its pointer
// slabs for response identity coverage. The slabs share the protobuf budget.
func HasResponseEncoding(ctx context.Context) bool {
	_, ok := ctx.Value(responseEncodingRunKey{}).(*responseEncodingRun)
	return ok
}

// RecordResponseEncoding records an already preflighted batch and its wire-size
// bound. Callers retain the pointer slab without reusing it for another batch.
func RecordResponseEncoding(ctx context.Context, points []*measurev1.InternalDataPoint, bound uint64, covered bool) error {
	run, ok := ctx.Value(responseEncodingRunKey{}).(*responseEncodingRun)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	run.scope.mu.Lock()
	defer run.scope.mu.Unlock()
	if run.scope.encoding != run {
		return nil
	}
	if bound > math.MaxInt64-run.bound || uint64(len(points)) > math.MaxUint64-run.rows {
		return ErrMemoryBudgetExceeded
	}
	run.bound += bound
	run.rows += uint64(len(points))
	run.covered = run.covered && covered
	if run.covered {
		run.batches = append(run.batches, points)
	} else {
		run.batches = nil
	}
	return nil
}

// FinishResponseEncoding marks a drained final serialization complete, or
// revokes coverage on error or early Close. Completed coverage survives Close.
func FinishResponseEncoding(ctx context.Context, success bool) {
	run, ok := ctx.Value(responseEncodingRunKey{}).(*responseEncodingRun)
	if !ok {
		return
	}
	run.scope.mu.Lock()
	defer run.scope.mu.Unlock()
	if run.scope.encoding == run {
		run.complete = success
		if !success {
			run.covered = false
			run.batches = nil
		}
	}
}

func responseEncodingBound(run *responseEncodingRun, response *measurev1.QueryResponse) (uint64, bool) {
	if run == nil || !run.covered || !run.complete || run.rows != uint64(len(response.GetDataPoints())) {
		return 0, false
	}
	row := 0
	for _, batch := range run.batches {
		for _, point := range batch {
			if point == nil || point.DataPoint != response.DataPoints[row] {
				return 0, false
			}
			row++
		}
	}
	return run.bound, true
}

func responseEncodingSize(message proto.Message, run *responseEncodingRun) (uint64, error) {
	response, measureResponse := message.(*measurev1.QueryResponse)
	if measureResponse && response == nil {
		return 11, nil
	}
	var size uint64
	bound, covered := responseEncodingBound(run, response)
	if measureResponse && covered {
		// Construct the control envelope explicitly: copying MessageState would
		// copy protobuf synchronization state. No DataPoint is walked here.
		envelope := &measurev1.QueryResponse{Trace: response.Trace, GroupStatuses: response.GroupStatuses}
		envelope.ProtoReflect().SetUnknown(response.ProtoReflect().GetUnknown())
		size = uint64(proto.Size(envelope))
		if bound > math.MaxInt64-size {
			return 0, ErrMemoryBudgetExceeded
		}
		size += bound
	} else {
		size = uint64(proto.Size(message))
	}
	if measureResponse {
		// The QL oneof wraps this response after the measure finalizer. Its
		// tag and length prefix need at most eleven additional bytes.
		if size > math.MaxInt64-11 {
			return 0, ErrMemoryBudgetExceeded
		}
		size += 11
	}
	if size > math.MaxInt64/2 {
		return 0, ErrMemoryBudgetExceeded
	}
	return size, nil
}

// ReserveResponseEncoding reserves encoding and transport copies beside the retained
// protobuf response. A transport scope holds the reservation until RPC completion;
// direct callers must invoke the returned release after materialization.
func ReserveResponseEncoding(ctx context.Context, response any) (func(), error) {
	noop := func() {}
	var run *responseEncodingRun
	scope, hasScope := ctx.Value(queryMemoryScopeKey{}).(*queryMemoryScope)
	if hasScope {
		scope.mu.Lock()
		run, scope.encoding = scope.encoding, nil
		scope.mu.Unlock()
	}
	message, ok := response.(proto.Message)
	if !ok {
		return noop, nil
	}
	if err := ctx.Err(); err != nil {
		return noop, err
	}
	tracker, _ := ctx.Value(memoryTrackerContextKey{}).(*MemoryTracker)
	if tracker == nil {
		if hasScope {
			scope.mu.Lock()
			tracker = scope.tracker
			scope.mu.Unlock()
		}
	}
	if tracker == nil {
		if _, admitted := query.BudgetLeaseFromContext(ctx); !admitted {
			return noop, nil
		}
		size, sizeErr := responseEncodingSize(message, nil)
		if sizeErr != nil {
			return noop, sizeErr
		}
		return noop, query.Charge(ctx, size*2)
	}
	size, sizeErr := responseEncodingSize(message, run)
	if sizeErr != nil {
		return noop, sizeErr
	}
	bytes := int64(size * 2)
	// The estimate avoids walking DataPoints when headroom permits. Near either
	// limit, size exactly before charging: a conservative estimate must not
	// reject a response that fits, or spend a monotonic lease speculatively.
	fits := bytes <= tracker.limit-tracker.Used()
	if lease, admitted := query.BudgetLeaseFromContext(ctx); admitted {
		used, limit := lease.Used(), lease.Limit()
		fits = fits && used <= limit && uint64(bytes) <= limit-used
	}
	if run != nil && !fits {
		size, sizeErr = responseEncodingSize(message, nil)
		if sizeErr != nil {
			return noop, sizeErr
		}
		bytes = int64(size * 2)
	}
	if err := ctx.Err(); err != nil {
		return noop, err
	}
	if err := tracker.Reserve(bytes); err != nil {
		return noop, err
	}
	var once sync.Once
	release := func() { once.Do(func() { tracker.Release(bytes) }) }
	return query.RegisterTransportRelease(ctx, release), nil
}
