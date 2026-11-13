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

package measure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Rejection reasons recorded when the cursor a scan needs cannot be reserved. The protector's
// reasons (protector.Reason*) are used as they are.
const (
	rejectRawQuota      = "raw_quota"
	rejectSharedTracker = "shared_tracker"
	rejectAcquire       = "acquire"
	rejectOther         = "other"
)

// scanTelemetry is what one scan measured about itself: where its time went, how its decode
// pool and memory budget behaved. Decode workers write it, so every counter is atomic. It is
// always collected, because a few atomic adds per cursor cost nothing next to a decode; the
// trace span and the metrics are produced from it when the scan ends.
type scanTelemetry struct {
	lastReject string
	rejects    map[string]int64
	// deep asks for measurements that cost a pass over each decoded cursor; set only when
	// the query is traced.
	deep         atomic.Bool
	decodes      atomic.Int64
	decodesEmpty atomic.Int64
	decodeNS     atomic.Int64
	decodeNSMax  atomic.Int64
	queueNS      atomic.Int64
	hits         atomic.Int64
	waits        atomic.Int64
	waitNS       atomic.Int64
	planNS       atomic.Int64
	scanNS       atomic.Int64
	estimate     atomic.Int64
	estimateKept atomic.Int64
	retained     atomic.Int64
	compacted    atomic.Int64
	poolWorkers  int
	poolQueued   int
	mu           sync.Mutex
}

func (t *scanTelemetry) observeDecode(d time.Duration, ok bool) {
	t.decodes.Add(1)
	if !ok {
		t.decodesEmpty.Add(1)
	}
	ns := d.Nanoseconds()
	t.decodeNS.Add(ns)
	for cur := t.decodeNSMax.Load(); ns > cur && !t.decodeNSMax.CompareAndSwap(cur, ns); cur = t.decodeNSMax.Load() {
	}
}

// reject records a reservation the scan could not get, by reason, and returns the reason. An
// error that names none is recorded under fallback.
func (t *scanTelemetry) reject(err error, fallback string) string {
	reason := fallback
	detail := err.Error()
	var exhausted *protector.ExhaustedError
	switch {
	case errors.As(err, &exhausted):
		reason = exhausted.Reason
	case errors.Is(err, vectorized.ErrMemoryBudgetExceeded):
		reason = rejectSharedTracker
	}
	t.mu.Lock()
	if t.rejects == nil {
		t.rejects = make(map[string]int64)
	}
	t.rejects[reason]++
	t.lastReject = detail
	t.mu.Unlock()
	return reason
}

func (t *scanTelemetry) rejectSummary() (counts, last string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.rejects) == 0 {
		return "", ""
	}
	keys := make([]string, 0, len(t.rejects))
	for k := range t.rejects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, t.rejects[k]))
	}
	return strings.Join(parts, ","), t.lastReject
}

// snapshot reads the node pool's size and backlog.
func (p *decodePool) snapshot() (workers, queued int) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers, len(p.urgent) + len(p.queue) - p.head
}

// leasePeaker is implemented by leases that track the most bytes they held at once.
type leasePeaker interface{ Peak() uint64 }

// leaseUse reads the query lease's limit and peak from ctx.
func leaseUse(ctx context.Context) (limit, peak uint64, ok bool) {
	lease, found := query.BudgetLeaseFromContext(ctx)
	if !found {
		return 0, 0, false
	}
	if p, peaks := lease.(leasePeaker); peaks {
		peak = p.Peak()
	}
	return lease.Limit(), peak, true
}

// scanSpanName is the name of the storage scan span, apart from the "scan" operator span of the pipeline.
const scanSpanName = "scan-storage"

// scanSpan is the trace span of one scan, or nil when the query is not traced.
type scanSpan struct{ span *query.Span }

func startScanSpan(ctx context.Context) *scanSpan {
	tracer := query.GetTracer(ctx)
	if tracer == nil {
		return nil
	}
	span, _ := tracer.StartSpan(ctx, scanSpanName)
	return &scanSpan{span: span}
}

// finish tags the span with what the scan measured and stops it.
func (s *scanSpan) finish(ctx context.Context, st *scanStats, tel *scanTelemetry, budget *scanBudget) {
	if s == nil {
		return
	}
	sp := s.span
	num := func(key string, v int64) { sp.Tagf(key, "%d", v) }
	num(tracelabels.TagScanBlocks, st.blocks)
	num(tracelabels.TagScanBlocksDecoded, tel.decodes.Load()-tel.decodesEmpty.Load())
	num(tracelabels.TagScanBlocksNoRows, tel.decodesEmpty.Load())
	num(tracelabels.TagScanWindows, st.windows)
	num(tracelabels.TagMergeUnits, st.mergeUnits)
	num(tracelabels.TagMergeCursors, st.mergeCursors)
	num(tracelabels.TagMergePeakLive, st.mergePeakLive)
	num(tracelabels.TagPrefetchPeak, st.prefetchPeak)
	num(tracelabels.TagPrefetchHits, tel.hits.Load())
	num(tracelabels.TagPrefetchWaits, tel.waits.Load())
	num(tracelabels.TagDecodeWaitNS, tel.waitNS.Load())
	num(tracelabels.TagDecodeNSTotal, tel.decodeNS.Load())
	num(tracelabels.TagDecodeNSMax, tel.decodeNSMax.Load())
	num(tracelabels.TagDecodeQueueNS, tel.queueNS.Load())
	num(tracelabels.TagPlanNS, tel.planNS.Load())
	num(tracelabels.TagScanNS, tel.scanNS.Load())
	num(tracelabels.TagMergeNS, max(tel.scanNS.Load()-tel.waitNS.Load(), 0))
	num(tracelabels.TagPoolWorkers, int64(tel.poolWorkers))
	num(tracelabels.TagPoolQueued, int64(tel.poolQueued))
	num(tracelabels.TagCompactedCursors, tel.compacted.Load())
	if limit, peak, ok := leaseUse(ctx); ok {
		num(tracelabels.TagBudgetLeaseLimit, int64(limit))
		num(tracelabels.TagBudgetLeasePeak, int64(peak))
	}
	if tracker, ok := vectorized.QueryMemoryTrackerFromContext(ctx); ok {
		num(tracelabels.TagBudgetSharedLimit, tracker.Limit())
		num(tracelabels.TagBudgetSharedPeak, tracker.Peak())
	}
	num(tracelabels.TagBudgetEstimate, tel.estimate.Load())
	num(tracelabels.TagBudgetEstimateKept, tel.estimateKept.Load())
	num(tracelabels.TagBudgetRetained, tel.retained.Load())
	if counts, last := tel.rejectSummary(); counts != "" {
		sp.Tag(tracelabels.TagBudgetRejects, counts)
		sp.Tag(tracelabels.TagBudgetRejectReason, last)
	}
	if budget != nil && budget.sliceBudget > 0 {
		num(tracelabels.TagSliceNeed, budget.sliceNeed)
		num(tracelabels.TagSliceTarget, budget.sliceBudget)
	}
	num(tracelabels.TagSlices, st.slices)
	num(tracelabels.TagSlicedUnits, st.slicedUnits)
	sp.Stop()
}

// observeEnd feeds the scan's budget use to the metrics.
func observeScanEnd(ctx context.Context, qm *queryMetrics, st *scanStats) {
	if qm == nil {
		return
	}
	if limit, peak, ok := leaseUse(ctx); ok && limit > 0 && peak > 0 {
		qm.queryBudgetPeakRatio.Observe(float64(peak) / float64(limit))
	}
	if st.slices > 0 {
		qm.scanSlices.Inc(float64(st.slices))
		qm.scanSlicedUnits.Inc(float64(st.slicedUnits))
	}
}

// discardTelemetry takes the measurements of a budget that has none of its own.
var discardTelemetry = new(scanTelemetry)

// telemetry returns the budget's measurements, never nil.
func (b *scanBudget) telemetry() *scanTelemetry {
	if b == nil || b.tel == nil {
		return discardTelemetry
	}
	return b.tel
}
