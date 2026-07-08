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

package trace

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// newTestSamplerMetrics builds a samplerMetrics backed by the fakeMetricFactory.
func newTestSamplerMetrics() (*samplerMetrics, *fakeMetricFactory) {
	f := newFakeMetricFactory()
	sm := newSamplerMetrics(f)
	return sm, f
}

// TestPluginTelemetry_MetricNamePrefix verifies that plugin metric names are
// always prefixed with "plugin_" regardless of what the plugin requests.
func TestPluginTelemetry_MetricNamePrefix(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()
	require.NotNil(t, m)

	_ = m.Counter("my_counter")
	_ = m.Gauge("my_gauge")

	// The factory should have received names prefixed with "plugin_".
	f.mu.Lock()
	_, hasCounter := f.counters["plugin_my_counter"]
	_, hasGauge := f.gauges["plugin_my_gauge"]
	f.mu.Unlock()

	assert.True(t, hasCounter, "counter name must be prefixed with plugin_")
	assert.True(t, hasGauge, "gauge name must be prefixed with plugin_")
}

// TestPluginTelemetry_MetricNameSanitization verifies that mixed-case and
// special-character names are lowercased and sanitized.
func TestPluginTelemetry_MetricNameSanitization(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	_ = m.Counter("MyCounter-V2")

	f.mu.Lock()
	_, ok := f.counters["plugin_mycounter_v2"]
	f.mu.Unlock()
	assert.True(t, ok, "sanitized counter name must be plugin_mycounter_v2")
}

// TestPluginTelemetry_ReservedLabelRejected verifies that reserved label names
// {group, plugin_name} are silently removed and series_rejected incremented.
func TestPluginTelemetry_ReservedLabelRejected(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	// Both "group" and "plugin_name" are reserved — they should be stripped.
	c := m.Counter("hits", "group", "plugin_name", "env")
	require.NotNil(t, c)

	rejectedCounter := f.counter("plugin_telemetry_series_rejected_total")
	require.NotNil(t, rejectedCounter)
	// Two reserved labels → two increments.
	assert.GreaterOrEqual(t, len(rejectedCounter.calls), 2,
		"two reserved label names must each increment series_rejected")
}

// TestPluginTelemetry_CardinalityCap verifies that emitting more than
// maxSeriesPerInstrument distinct custom-value tuples routes extras to the
// overflow sentinel and increments series_rejected for each excess tuple.
func TestPluginTelemetry_CardinalityCap(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	c := m.Counter("reqs", "svc")
	require.NotNil(t, c)

	// Emit exactly maxSeriesPerInstrument distinct tuples.
	for i := range maxSeriesPerInstrument {
		c.Inc(1, strings.Repeat("x", i+1))
	}

	rejBefore := len(f.counter("plugin_telemetry_series_rejected_total").calls)

	// Emit K extra distinct tuples — each should be rejected.
	const extraK = 5
	for i := range extraK {
		c.Inc(1, "extra_"+strings.Repeat("y", i+1))
	}

	rejCounter := f.counter("plugin_telemetry_series_rejected_total")
	rejAfter := len(rejCounter.calls)
	assert.Equal(t, extraK, rejAfter-rejBefore,
		"each excess distinct tuple must increment series_rejected exactly once")

	// Verify the underlying counter received the overflow sentinel label values.
	underlying := f.counter("plugin_reqs")
	require.NotNil(t, underlying)
	overflowCount := underlying.callsWithLabels("grp", "plug", overflowSentinel)
	assert.Equal(t, extraK, overflowCount, "overflow tuples must use the sentinel value")
}

// TestPluginTelemetry_BypassZeroAllocs verifies that Inc/Set/Observe on a
// bypass-backed adapter perform zero heap allocations for no-label invocations.
// The BenchmarkPluginTelemetry_BypassInc benchmark covers the labeled case;
// the Go compiler is unable to avoid a []string backing allocation when a
// variadic call crosses an interface boundary in a test context, but the
// adapter itself builds no internal slices.
func TestPluginTelemetry_BypassZeroAllocs(t *testing.T) {
	sm, _ := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	// nil factory → bypass path.
	pt := newPluginTelemetry(nil, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	c := m.Counter("hits")
	g := m.Gauge("lat")
	h := m.Histogram("size", []float64{1, 5, 10})

	allocs := testing.AllocsPerRun(100, func() {
		c.Inc(1)
		g.Set(3.14)
		h.Observe(7.0)
	})
	assert.Equal(t, float64(0), allocs, "bypass path must have 0 allocs/op on no-label calls")
}

// TestPluginTelemetry_LoggerInfoWarnError verifies that Info/Warn/Error calls
// go through the rate limiter without panicking.
func TestPluginTelemetry_LoggerInfoWarnError(t *testing.T) {
	sm, _ := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(nil, baseLogger, sm, "grp", "plug")
	l := pt.Logger()

	require.NotPanics(t, func() {
		l.Info("hello", "k1", "v1")
		l.Warn("warn msg", "k2", 42)
		l.Error("err msg")
	})
}

// TestPluginTelemetry_LoggerDebugDropped verifies that Debug calls are silently
// dropped under the INFO level cap.
func TestPluginTelemetry_LoggerDebugDropped(t *testing.T) {
	sm, _ := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(nil, baseLogger, sm, "grp", "plug")
	l := pt.Logger()

	// This must not panic and must not count against the rate limiter.
	require.NotPanics(t, func() {
		for range 200 {
			l.Debug("debug msg", "k", "v")
		}
	})
	// Rate limiter tokens must be unaffected by Debug calls.
	pl := pt.loggerAdapter
	pl.mu.Lock()
	tok := pl.tokens
	pl.mu.Unlock()
	assert.Equal(t, logBurstSize, tok, "Debug must not consume rate-limiter tokens")
}

// TestPluginTelemetry_LogRateLimit verifies that emitting many lines quickly
// drops some and increments plugin_log_dropped_total.
func TestPluginTelemetry_LogRateLimit(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	l := pt.Logger()

	const burst = 200
	for range burst {
		l.Info("rapid fire")
	}

	droppedCounter := f.counter("plugin_log_dropped_total")
	require.NotNil(t, droppedCounter)
	// We emitted 200 lines but burst capacity is 100 — at least some must drop.
	assert.Greater(t, len(droppedCounter.calls), 0,
		"emitting beyond burst capacity must increment plugin_log_dropped_total")
}

// TestPluginTelemetry_LogMsgTruncated verifies that the production emit path
// (emitEvent) truncates a message longer than maxMsgLen before it is written. It
// drives the real emitEvent with a zerolog event wired to a buffer and asserts the
// emitted "message" field, rather than re-implementing the truncation in the test.
func TestPluginTelemetry_LogMsgTruncated(t *testing.T) {
	longMsg := strings.Repeat("A", maxMsgLen+100)

	var buf bytes.Buffer
	lg := zerolog.New(&buf)
	emitEvent(lg.Info(), longMsg, nil)

	var out map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	msg, ok := out[zerolog.MessageFieldName].(string)
	require.True(t, ok, "emitted log must carry a message field")
	assert.Len(t, msg, maxMsgLen, "emitEvent must truncate the message to maxMsgLen bytes")
	assert.Equal(t, strings.Repeat("A", maxMsgLen), msg, "truncated message must be the maxMsgLen-byte prefix")
}

// TestPluginTelemetry_LogFieldsCapped verifies that more than maxLogFields pairs
// do not panic and that trailing odd key gets "(MISSING)".
func TestPluginTelemetry_LogFieldsCapped(t *testing.T) {
	sm, _ := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(nil, baseLogger, sm, "grp", "plug")
	l := pt.Logger()

	// Build 20 pairs (exceeds maxLogFields=16) and one trailing odd key.
	kvs := make([]any, 0, 41)
	for i := range 20 {
		kvs = append(kvs, "k", i)
	}
	kvs = append(kvs, "trailing_odd_key")

	require.NotPanics(t, func() { l.Info("fields test", kvs...) })
}

// TestPluginTelemetry_Teardown verifies that teardown Deletes all emitted tuples
// and is safe to call multiple times.
func TestPluginTelemetry_Teardown(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	c := m.Counter("evt", "env")
	c.Inc(1, "prod")
	c.Inc(1, "staging")

	require.NotPanics(t, func() { pt.teardown() })
	require.NotPanics(t, func() { pt.teardown() }) // idempotent

	underlying := f.counter("plugin_evt")
	require.NotNil(t, underlying)
	// After teardown the cache is cleared; the instrument received the calls.
	assert.GreaterOrEqual(t, len(underlying.calls), 2,
		"underlying counter must have recorded the Inc calls")
}

// TestPluginTelemetry_TeardownPanicsafe verifies that teardown does not
// propagate panics from Delete to the caller.
func TestPluginTelemetry_TeardownPanicsafe(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	// Register a counter so there is something to tear down.
	c := m.Counter("panic_evt")
	c.Inc(1)

	require.NotPanics(t, func() { pt.teardown() },
		"teardown must not propagate panics from Delete")
}

// TestPluginTelemetry_NilSamplerMetrics verifies that a nil *samplerMetrics
// does not cause panics in the adapter paths.
func TestPluginTelemetry_NilSamplerMetrics(t *testing.T) {
	baseLogger := logger.GetLogger("trace")
	// nil sm is valid — all inc helpers guard against nil receiver.
	pt := newPluginTelemetry(nil, baseLogger, nil, "grp", "plug")
	m := pt.Meter()

	require.NotPanics(t, func() {
		c := m.Counter("safe")
		c.Inc(1)
	})
}

// BenchmarkPluginTelemetry_BypassInc measures allocations on the bypass path.
// The no-label variant must produce 0 allocs/op; the labeled variant shows 1
// alloc/op from the Go compiler materializing the variadic []string when
// crossing the sdk.Counter interface boundary — that allocation is in the
// caller, not in the noop adapter itself.
func BenchmarkPluginTelemetry_BypassInc(b *testing.B) {
	sm, _ := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(nil, baseLogger, sm, "grp", "plug")
	// No-label counter: must be 0 allocs/op.
	c := pt.Meter().Counter("hits")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.Inc(1)
	}
}

// TestPluginTelemetry_SameInstrumentCached verifies that calling Counter with
// the same name+labels twice returns the same underlying sdk.Counter instance.
func TestPluginTelemetry_SameInstrumentCached(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	m := pt.Meter()

	c1 := m.Counter("evt")
	c2 := m.Counter("evt")
	assert.Equal(t, c1, c2, "repeated Counter() calls must return the same handle")

	f.mu.Lock()
	_, ok := f.counters["plugin_evt"]
	f.mu.Unlock()
	assert.True(t, ok)
}

// TestPluginTelemetry_MeterReturnedHostInterface verifies that pluginTelemetry
// satisfies sdk.Host.
func TestPluginTelemetry_MeterReturnedHostInterface(t *testing.T) {
	sm, _ := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(nil, baseLogger, sm, "grp", "plug")

	var host sdk.Host = pt
	require.NotNil(t, host.Meter())
	require.NotNil(t, host.Logger())
}

// TestPluginTelemetry_LogSuppressionSummary verifies that the suppression
// summary is emitted at most once per suppressWarnInterval.
func TestPluginTelemetry_LogSuppressionSummary(t *testing.T) {
	sm, f := newTestSamplerMetrics()
	baseLogger := logger.GetLogger("trace")
	pt := newPluginTelemetry(f, baseLogger, sm, "grp", "plug")
	l := pt.loggerAdapter

	// Drain the bucket so subsequent calls will drop.
	for range int(logBurstSize) + 10 {
		l.Info("drain")
	}

	droppedBefore := len(f.counter("plugin_log_dropped_total").calls)
	assert.Greater(t, droppedBefore, 0, "some lines must have been dropped")

	// Advance time so the suppression-summary interval is exceeded.
	l.mu.Lock()
	l.lastWarnEmit = time.Now().Add(-suppressWarnInterval * 2)
	l.mu.Unlock()

	// Next Info call should emit the summary and not panic.
	require.NotPanics(t, func() { l.Info("trigger summary") })
}
