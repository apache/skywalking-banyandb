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

package protector

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/cgroups"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// TestStateConstants verifies state constants are properly defined.
func TestStateConstants(t *testing.T) {
	assert.Equal(t, State(0), StateLow)
	assert.Equal(t, State(1), StateHigh)
}

// TestMemoryStateLow verifies StateLow when memory is plentiful.
func TestMemoryStateLow(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	m.limit.Store(1000) // 1KB limit

	// 80% available (800 bytes) > 20% threshold (200 bytes)
	atomic.StoreUint64(&m.usage, 200)
	assert.Equal(t, StateLow, m.State())
}

// TestMemoryStateHigh verifies StateHigh when memory is scarce.
func TestMemoryStateHigh(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	m.limit.Store(1000) // 1KB limit

	// 15% available (150 bytes) <= 20% threshold (200 bytes)
	atomic.StoreUint64(&m.usage, 850)
	assert.Equal(t, StateHigh, m.State())
}

// TestMemoryStateHighEdgeCase verifies StateHigh when no memory available.
func TestMemoryStateHighEdgeCase(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	m.limit.Store(1000)
	atomic.StoreUint64(&m.usage, 1000) // Exactly at limit
	assert.Equal(t, StateHigh, m.State())
}

// TestMemoryStateNoLimit verifies behavior when no limit is set.
func TestMemoryStateNoLimit(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	// No limit set (0)
	assert.Equal(t, StateLow, m.State()) // Fail open
}

// TestCgroupLimitGaugeValue verifies the raw cgroup limit is mapped to a gauge value,
// reporting 0 for unlimited/unreadable/implausible inputs.
func TestCgroupLimitGaugeValue(t *testing.T) {
	assert.Equal(t, float64(4<<30), cgroupLimitGaugeValue(4<<30, nil)) // 4GiB limit
	assert.Equal(t, float64(0), cgroupLimitGaugeValue(-1, nil))        // cgroup memory.max == "max"
	assert.Equal(t, float64(0), cgroupLimitGaugeValue(0, nil))
	assert.Equal(t, float64(0), cgroupLimitGaugeValue(int64(2e18), nil))       // implausibly large
	assert.Equal(t, float64(0), cgroupLimitGaugeValue(4<<30, errors.New("x"))) // read error
}

// TestMetricsCreatedInPreRunNotConstructor pins the bug fix: NewMemory must NOT create
// metrics (the prometheus provider is still nil during command assembly, so a gauge
// created then is a silent no-op); PreRun creates all four against a ready provider.
func TestMetricsCreatedInPreRunNotConstructor(t *testing.T) {
	reg := newFakeRegistry()
	m := NewMemory(reg).(*memory)

	assert.Equal(t, 0, len(reg.created), "constructor must not create any metric")
	assert.Nil(t, m.limitGauge)
	assert.Nil(t, m.usageGauge)
	assert.Nil(t, m.cgroupLimitGauge)
	assert.Nil(t, m.overLimitCounter)

	require := assert.New(t)
	require.NoError(m.PreRun(context.Background()))

	assert.NotNil(t, m.limitGauge)
	assert.NotNil(t, m.usageGauge)
	assert.NotNil(t, m.cgroupLimitGauge)
	assert.NotNil(t, m.overLimitCounter)
	assert.Contains(t, reg.created, "banyandb_memory_protector/limit")
	assert.Contains(t, reg.created, "banyandb_memory_protector/usage")
	assert.Contains(t, reg.created, "banyandb_memory_protector/over_limit_total")
	assert.Contains(t, reg.created, "banyandb_memory_protector/cgroup_limit_bytes")

	// The cgroup gauge is Set to the mapped raw cgroup limit (0 when unavailable/unlimited);
	// comparing against the same mapping keeps the assertion environment-independent.
	cgLimit, cgErr := cgroups.MemoryLimit()
	assert.Equal(t, cgroupLimitGaugeValue(cgLimit, cgErr), m.cgroupLimitGauge.(*fakeGauge).value)
}

// fakeRegistry is a capturing observability.MetricsRegistry test double that records
// the fully-qualified name of every metric created through it.
type fakeRegistry struct {
	created map[string]struct{}
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{created: make(map[string]struct{})}
}

func (r *fakeRegistry) With(scope meter.Scope) observability.Factory {
	return &fakeFactory{reg: r, namespace: scope.GetNamespace()}
}
func (r *fakeRegistry) NativeEnabled() bool   { return false }
func (r *fakeRegistry) Name() string          { return "fake-metrics-registry" }
func (r *fakeRegistry) Serve() run.StopNotify { return nil }
func (r *fakeRegistry) GracefulStop()         {}

type fakeFactory struct {
	reg       *fakeRegistry
	namespace string
}

func (f *fakeFactory) record(name string) {
	f.reg.created[f.namespace+"/"+name] = struct{}{}
}

func (f *fakeFactory) NewCounter(name string, _ ...string) meter.Counter {
	f.record(name)
	return &fakeCounter{}
}

func (f *fakeFactory) NewGauge(name string, _ ...string) meter.Gauge {
	f.record(name)
	return &fakeGauge{}
}

func (f *fakeFactory) NewHistogram(name string, _ meter.Buckets, _ ...string) meter.Histogram {
	f.record(name)
	return &fakeHistogram{}
}
func (f *fakeFactory) Close() {}

type fakeGauge struct{ value float64 }

func (g *fakeGauge) Set(v float64, _ ...string) { g.value = v }
func (g *fakeGauge) Add(v float64, _ ...string) { g.value += v }
func (g *fakeGauge) Delete(_ ...string) bool    { return true }

type fakeCounter struct{ value float64 }

func (c *fakeCounter) Inc(v float64, _ ...string) { c.value += v }
func (c *fakeCounter) Delete(_ ...string) bool    { return true }

type fakeHistogram struct{}

func (h *fakeHistogram) Observe(_ float64, _ ...string) {}
func (h *fakeHistogram) Delete(_ ...string) bool        { return true }
