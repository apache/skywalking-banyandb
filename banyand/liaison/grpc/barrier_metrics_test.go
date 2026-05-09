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

package grpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// recordingFactory is a minimal observability.Factory that captures every
// Counter.Inc / Histogram.Observe / Gauge.Set call for assertion. Tests use
// it to pin the schema-barrier metric labels emitted by the production
// instrumentation without depending on Prometheus or the bypass registry's
// no-op semantics.
type recordingFactory struct {
	counters   map[string]*recordingCounter
	histograms map[string]*recordingHistogram
	gauges     map[string]*recordingGauge
	mu         sync.Mutex
}

func newRecordingFactory() *recordingFactory {
	return &recordingFactory{
		counters:   map[string]*recordingCounter{},
		histograms: map[string]*recordingHistogram{},
		gauges:     map[string]*recordingGauge{},
	}
}

func (f *recordingFactory) NewCounter(name string, _ ...string) meter.Counter {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &recordingCounter{}
	f.counters[name] = c
	return c
}

func (f *recordingFactory) NewHistogram(name string, _ meter.Buckets, _ ...string) meter.Histogram {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := &recordingHistogram{}
	f.histograms[name] = h
	return h
}

func (f *recordingFactory) NewGauge(name string, _ ...string) meter.Gauge {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := &recordingGauge{}
	f.gauges[name] = g
	return g
}

func (f *recordingFactory) Close() {}

func (f *recordingFactory) counter(name string) *recordingCounter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counters[name]
}

func (f *recordingFactory) histogram(name string) *recordingHistogram {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.histograms[name]
}

type recordedCall struct {
	labels []string
	value  float64
}

type recordingCounter struct {
	calls []recordedCall
	mu    sync.Mutex
}

func (r *recordingCounter) Inc(delta float64, labelValues ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedCall{value: delta, labels: append([]string{}, labelValues...)})
}

func (r *recordingCounter) Delete(_ ...string) bool { return true }

func (r *recordingCounter) snapshot() []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedCall, len(r.calls))
	copy(out, r.calls)
	return out
}

type recordingHistogram struct {
	calls []recordedCall
	mu    sync.Mutex
}

func (r *recordingHistogram) Observe(value float64, labelValues ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedCall{value: value, labels: append([]string{}, labelValues...)})
}

func (r *recordingHistogram) Delete(_ ...string) bool { return true }

func (r *recordingHistogram) snapshot() []recordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedCall, len(r.calls))
	copy(out, r.calls)
	return out
}

type recordingGauge struct{}

func (r *recordingGauge) Set(_ float64, _ ...string) {}
func (r *recordingGauge) Add(_ float64, _ ...string) {}
func (r *recordingGauge) Delete(_ ...string) bool    { return true }

var _ observability.Factory = (*recordingFactory)(nil)

func newRecordingMetrics(t *testing.T) (*recordingFactory, *metrics) {
	t.Helper()
	f := newRecordingFactory()
	return f, newMetrics(f)
}

// fakeBarrierCache satisfies barrierCacheReader with controllable revision
// + per-key state for the standalone path tests below.
type fakeBarrierCache struct {
	keyRevs map[string]int64
	maxRev  int64
}

func (f fakeBarrierCache) GetMaxModRevision() int64 {
	return f.maxRev
}

func (f fakeBarrierCache) GetKeyModRevision(propID string) (int64, bool) {
	if f.keyRevs == nil {
		return 0, false
	}
	rev, ok := f.keyRevs[propID]
	return rev, ok
}

// --- Helper-level tests --------------------------------------------------.

func TestBarrierResultLabel_AppliedTrue_ReturnsApplied(t *testing.T) {
	assert.Equal(t, resultLabelApplied, barrierResultLabel(true, nil))
}

func TestBarrierResultLabel_AppliedFalseNoErr_ReturnsTimeout(t *testing.T) {
	assert.Equal(t, resultLabelTimeout, barrierResultLabel(false, nil))
}

func TestBarrierResultLabel_InvalidArgument_ReturnsInvalidArgument(t *testing.T) {
	err := status.Errorf(codes.InvalidArgument, "too many keys")
	assert.Equal(t, resultLabelInvalidArgument, barrierResultLabel(false, err))
}

func TestBarrierResultLabel_OtherError_ReturnsError(t *testing.T) {
	err := status.Errorf(codes.Internal, "boom")
	assert.Equal(t, resultLabelError, barrierResultLabel(false, err))
}

func TestSplitRoleNode_LiaisonPrefix(t *testing.T) {
	role, name := splitRoleNode("liaison-foo")
	assert.Equal(t, "liaison", role)
	assert.Equal(t, "foo", name)
}

func TestSplitRoleNode_DataPrefix(t *testing.T) {
	role, name := splitRoleNode("data-bar")
	assert.Equal(t, "data", role)
	assert.Equal(t, "bar", name)
}

func TestSplitRoleNode_NoPrefix_FallsBackToSelf(t *testing.T) {
	role, name := splitRoleNode("nodash")
	assert.Equal(t, roleLabelSelf, role)
	assert.Equal(t, "nodash", name)
}

func TestSplitRoleNode_Empty_FallsBackToSelf(t *testing.T) {
	role, name := splitRoleNode("")
	assert.Equal(t, roleLabelSelf, role)
	assert.Equal(t, "", name)
}

func TestSplitRoleNode_DataWithEmbeddedDashes_PreservesTail(t *testing.T) {
	role, name := splitRoleNode("data-host-with-dashes")
	assert.Equal(t, "data", role)
	assert.Equal(t, "host-with-dashes", name)
}

func TestRecordBarrierLaggards_NilCounter_NoPanic(_ *testing.T) {
	// Defensive: empty laggards on nil counter must not panic.
	recordBarrierLaggards(nil, barrierLabelRevisionApplied, []*schemav1.NodeLaggard{{Node: "data-bar"}})
}

func TestRecordBarrierLaggards_BumpsPerLaggardWithRoleSplit(t *testing.T) {
	f, m := newRecordingMetrics(t)
	recordBarrierLaggards(m.schemaBarrierLaggards, barrierLabelSchemaApplied, []*schemav1.NodeLaggard{
		{Node: "liaison-foo"},
		{Node: "data-bar"},
		{Node: "noprefix"},
	})
	calls := f.counter("schema_barrier_laggard_nodes_total").snapshot()
	require.Len(t, calls, 3)
	assert.Equal(t, []string{barrierLabelSchemaApplied, "liaison", "foo"}, calls[0].labels)
	assert.Equal(t, []string{barrierLabelSchemaApplied, "data", "bar"}, calls[1].labels)
	assert.Equal(t, []string{barrierLabelSchemaApplied, roleLabelSelf, "noprefix"}, calls[2].labels)
}

// --- Barrier integration tests ------------------------------------------.

func newRecordingBarrier(t *testing.T, cache barrierCacheReader) (*barrierService, *recordingFactory) {
	t.Helper()
	f, m := newRecordingMetrics(t)
	b := newBarrierService(func() barrierCacheReader { return cache })
	b.metrics = m
	b.l = logger.GetLogger("test-barrier")
	return b, f
}

func TestAwaitRevisionApplied_AppliedRecordsAppliedHistogram(t *testing.T) {
	b, f := newRecordingBarrier(t, fakeBarrierCache{maxRev: 100})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	resp, err := b.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied())

	hist := f.histogram("schema_await_revision_applied_duration_seconds")
	require.NotNil(t, hist)
	calls := hist.snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{resultLabelApplied}, calls[0].labels)

	lag := f.counter("schema_barrier_laggard_nodes_total").snapshot()
	assert.Empty(t, lag, "applied=true response carries no laggards")
}

func TestAwaitRevisionApplied_TimeoutRecordsTimeoutAndLaggard(t *testing.T) {
	b, f := newRecordingBarrier(t, fakeBarrierCache{maxRev: 50})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	resp, err := b.AwaitRevisionApplied(ctx, &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(30 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())

	calls := f.histogram("schema_await_revision_applied_duration_seconds").snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{resultLabelTimeout}, calls[0].labels)

	lag := f.counter("schema_barrier_laggard_nodes_total").snapshot()
	require.Len(t, lag, 1, "standalone timeout emits a single self laggard")
	assert.Equal(t, []string{barrierLabelRevisionApplied, roleLabelSelf, ""}, lag[0].labels)
}

func TestAwaitSchemaApplied_TooManyKeys_RecordsInvalidArgument(t *testing.T) {
	b, f := newRecordingBarrier(t, fakeBarrierCache{maxRev: 0})

	keys := make([]*schemav1.SchemaKey, barrierMaxKeys+1)
	for i := range keys {
		keys[i] = &schemav1.SchemaKey{Kind: "measure", Group: "g", Name: "m"}
	}
	resp, err := b.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{Keys: keys})
	require.Error(t, err, "expected InvalidArgument when keys exceed cap")
	require.Nil(t, resp)

	calls := f.histogram("schema_await_schema_applied_duration_seconds").snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{resultLabelInvalidArgument}, calls[0].labels)
}

func TestAwaitSchemaDeleted_AllAbsent_RecordsAppliedHistogram(t *testing.T) {
	b, f := newRecordingBarrier(t, fakeBarrierCache{maxRev: 0})

	resp, err := b.AwaitSchemaDeleted(context.Background(), &schemav1.AwaitSchemaDeletedRequest{
		Keys:    []*schemav1.SchemaKey{{Kind: "measure", Group: "g", Name: "m"}},
		Timeout: durationpb.New(20 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied())

	calls := f.histogram("schema_await_schema_deleted_duration_seconds").snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{resultLabelApplied}, calls[0].labels)
}

// --- Status counter integration -----------------------------------------.

func newRecordingMeasureService(t *testing.T, er *entityRepo, maxWait time.Duration) (*measureService, *recordingFactory) {
	t.Helper()
	f, m := newRecordingMetrics(t)
	return &measureService{
		discoveryService: &discoveryService{entityRepo: er},
		maxWaitDuration:  maxWait,
		l:                logger.GetLogger("test"),
		metrics:          m,
	}, f
}

func TestWriteGate_Measure_StaleClient_BumpsExpiredCounter(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id)
	svc, f := newRecordingMeasureService(t, er, 30*time.Millisecond)
	mock := &mockBidiServer[measurev1.WriteRequest, measurev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "m", ModRevision: 50}
	st := svc.validateWriteRequest(validMeasureWriteRequest(), meta, mock)
	require.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, st)

	calls := f.counter("schema_status_expired_schema_total").snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{rpcLabelMeasureWrite, "g"}, calls[0].labels)

	notApplied := f.counter("schema_status_schema_not_applied_total").snapshot()
	assert.Empty(t, notApplied, "stale-client path must not bump the not-applied counter")
}

func TestWriteGate_Measure_AheadClient_TimesOut_BumpsNotAppliedCounter(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id)
	svc, f := newRecordingMeasureService(t, er, 30*time.Millisecond)
	mock := &mockBidiServer[measurev1.WriteRequest, measurev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "m", ModRevision: 9999}
	st := svc.validateWriteRequest(validMeasureWriteRequest(), meta, mock)
	require.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, st)

	calls := f.counter("schema_status_schema_not_applied_total").snapshot()
	require.Len(t, calls, 1)
	assert.Equal(t, []string{rpcLabelMeasureWrite, "g", statusReasonWaitTimeout}, calls[0].labels)

	expired := f.counter("schema_status_expired_schema_total").snapshot()
	assert.Empty(t, expired, "ahead-client path must not bump the expired counter")
}

func TestRecordQueryGateStatuses_BumpsPerGroupCounters(t *testing.T) {
	f, m := newRecordingMetrics(t)
	statuses := map[string]modelv1.Status{
		"g_succ":    modelv1.Status_STATUS_SUCCEED,
		"g_expired": modelv1.Status_STATUS_EXPIRED_SCHEMA,
		"g_lag":     modelv1.Status_STATUS_SCHEMA_NOT_APPLIED,
		"g_other":   modelv1.Status_STATUS_NOT_FOUND,
	}
	recordQueryGateStatuses(m, rpcLabelMeasureQuery, statuses)

	expired := f.counter("schema_status_expired_schema_total").snapshot()
	require.Len(t, expired, 1)
	assert.Equal(t, []string{rpcLabelMeasureQuery, "g_expired"}, expired[0].labels)

	notApplied := f.counter("schema_status_schema_not_applied_total").snapshot()
	require.Len(t, notApplied, 1)
	assert.Equal(t, []string{rpcLabelMeasureQuery, "g_lag", statusReasonWaitTimeout}, notApplied[0].labels)
}

func TestRecordQueryGateStatuses_NilMetrics_NoPanic(_ *testing.T) {
	recordQueryGateStatuses(nil, rpcLabelMeasureQuery, map[string]modelv1.Status{
		"g": modelv1.Status_STATUS_EXPIRED_SCHEMA,
	})
}
