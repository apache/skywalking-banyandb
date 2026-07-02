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

package pressureprofiler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type fakeSource struct {
	rss   float64
	limit float64
	okRSS bool
}

func (f *fakeSource) LatestValues(names []string, _ map[string]string) map[string]float64 {
	out := make(map[string]float64, len(names))
	for _, name := range names {
		switch name {
		case rssMetricName:
			if f.okRSS {
				out[name] = f.rss
			}
		case limitMetricName:
			if f.limit > 0 {
				out[name] = f.limit
			}
		}
	}
	return out
}

func newProfilerForTest(t *testing.T, dir, port string, src *fakeSource) (*Profiler, *fakeProvider) {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	provider := newFakeProvider()
	p, err := New(Config{
		Dir:            dir,
		PprofPort:      port,
		PodName:        "demo-pod",
		RoleProvider:   func() string { return "data" },
		Cooldown:       time.Minute,
		TriggerPercent: 75,
		MaxArtifacts:   16,
	}, src, provider, logger.GetLogger("pressure-test"))
	require.NoError(t, err)
	return p, provider
}

func pprofTestServer(t *testing.T) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("PPROF-BYTES"))
	}))
	port := srv.URL[strings.LastIndex(srv.URL, ":")+1:]
	return port, srv.Close
}

// TestOnPollCompleteCapturesOnPressure captures both profiles once RSS/limit crosses the threshold.
func TestOnPollCompleteCapturesOnPressure(t *testing.T) {
	port, stop := pprofTestServer(t)
	defer stop()
	p, provider := newProfilerForTest(t, t.TempDir(), port, &fakeSource{rss: 800, limit: 1000, okRSS: true})

	p.OnPollComplete(context.Background())

	require.Eventually(t, func() bool {
		return provider.counter("pressure_capture_total").value() == 1
	}, 3*time.Second, 20*time.Millisecond)

	records := p.ListProfileRecords()
	require.Len(t, records, 1)
	assert.Equal(t, uint64(800), records[0].RSSBytes)
	assert.Equal(t, uint64(750), records[0].ThresholdBytes)
	assert.Len(t, records[0].Profiles, 2)
}

// TestOnPollCompleteBelowThreshold does nothing when RSS is under the threshold.
func TestOnPollCompleteBelowThreshold(t *testing.T) {
	port, stop := pprofTestServer(t)
	defer stop()
	p, provider := newProfilerForTest(t, t.TempDir(), port, &fakeSource{rss: 700, limit: 1000, okRSS: true})

	p.OnPollComplete(context.Background())
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(0), provider.counter("pressure_capture_total").value())
	assert.Empty(t, p.ListProfileRecords())
}

// TestOnPollCompleteCooldown skips a second capture inside the cooldown window.
func TestOnPollCompleteCooldown(t *testing.T) {
	port, stop := pprofTestServer(t)
	defer stop()
	p, provider := newProfilerForTest(t, t.TempDir(), port, &fakeSource{rss: 900, limit: 1000, okRSS: true})

	p.OnPollComplete(context.Background())
	require.Eventually(t, func() bool {
		return provider.counter("pressure_capture_total").value() == 1
	}, 3*time.Second, 20*time.Millisecond)

	// A second poll inside the cooldown window is skipped and counted.
	p.OnPollComplete(context.Background())
	assert.Equal(t, int64(1), provider.counter("pressure_skipped_cooldown_total").value())
	assert.Equal(t, int64(1), provider.counter("pressure_capture_total").value())
}

// TestOnPollCompleteMissingMetrics does nothing when the metrics are unavailable.
func TestOnPollCompleteMissingMetrics(t *testing.T) {
	port, stop := pprofTestServer(t)
	defer stop()
	p, provider := newProfilerForTest(t, t.TempDir(), port, &fakeSource{rss: 900, limit: 0, okRSS: true})

	p.OnPollComplete(context.Background())
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(0), provider.counter("pressure_capture_total").value())
}

// TestOnPollCompleteFinalizesOnFetchFailure verifies that when the pprof endpoint is unreachable
// the event is still finalized (with zero profiles) and the failures counter is incremented.
func TestOnPollCompleteFinalizesOnFetchFailure(t *testing.T) {
	// Port 1 has nothing listening, so every pprof fetch fails fast with connection refused.
	p, provider := newProfilerForTest(t, t.TempDir(), "1", &fakeSource{rss: 800, limit: 1000, okRSS: true})

	p.OnPollComplete(context.Background())

	require.Eventually(t, func() bool {
		return provider.counter("pressure_capture_total").value() == 1
	}, 5*time.Second, 20*time.Millisecond)
	assert.GreaterOrEqual(t, provider.counter("pressure_failures_total").value(), int64(1))

	records := p.ListProfileRecords()
	require.Len(t, records, 1)
	assert.Empty(t, records[0].Profiles, "event is finalized even when no profile could be fetched")
}

// fakeProvider is a capturing meter.Provider test double.

type fakeProvider struct {
	counters map[string]*fakeCounter
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{counters: make(map[string]*fakeCounter)}
}

func (p *fakeProvider) counter(name string) *fakeCounter {
	if c, ok := p.counters[name]; ok {
		return c
	}
	return &fakeCounter{}
}

func (p *fakeProvider) Counter(name string, _ ...string) meter.Counter {
	c := &fakeCounter{}
	p.counters[name] = c
	return c
}
func (p *fakeProvider) Gauge(_ string, _ ...string) meter.Gauge { return &fakeGauge{} }
func (p *fakeProvider) Histogram(_ string, _ meter.Buckets, _ ...string) meter.Histogram {
	return &fakeHistogram{}
}
func (p *fakeProvider) Close() {}

type fakeCounter struct{ n atomic.Int64 }

func (c *fakeCounter) Inc(delta float64, _ ...string) { c.n.Add(int64(delta)) }
func (c *fakeCounter) Delete(_ ...string) bool        { return true }
func (c *fakeCounter) value() int64                   { return c.n.Load() }

type fakeGauge struct{}

func (g *fakeGauge) Set(_ float64, _ ...string) {}
func (g *fakeGauge) Add(_ float64, _ ...string) {}
func (g *fakeGauge) Delete(_ ...string) bool    { return true }

type fakeHistogram struct{}

func (h *fakeHistogram) Observe(_ float64, _ ...string) {}
func (h *fakeHistogram) Delete(_ ...string) bool        { return true }
