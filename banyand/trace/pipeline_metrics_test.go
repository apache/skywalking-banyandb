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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// Fake meter primitives.

// labeledCall records one Inc or Set call with its label values.
type labeledCall struct {
	labels []string
	delta  float64
}

type fakeMetricCounter struct {
	calls   []labeledCall
	deletes [][]string
	mu      sync.Mutex
}

func (f *fakeMetricCounter) Inc(delta float64, labels ...string) {
	cp := make([]string, len(labels))
	copy(cp, labels)
	f.mu.Lock()
	f.calls = append(f.calls, labeledCall{delta: delta, labels: cp})
	f.mu.Unlock()
}

func (f *fakeMetricCounter) Delete(labels ...string) bool {
	cp := make([]string, len(labels))
	copy(cp, labels)
	f.mu.Lock()
	f.deletes = append(f.deletes, cp)
	f.mu.Unlock()
	return true
}

// deletesWithPrefix counts recorded Delete calls whose label slice starts with
// the given prefix.
func (f *fakeMetricCounter) deletesWithPrefix(prefix ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, d := range f.deletes {
		if len(d) < len(prefix) {
			continue
		}
		match := true
		for idx, lv := range prefix {
			if d[idx] != lv {
				match = false
				break
			}
		}
		if match {
			count++
		}
	}
	return count
}

func (f *fakeMetricCounter) callsWithLabels(labels ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, c := range f.calls {
		if len(c.labels) != len(labels) {
			continue
		}
		match := true
		for idx, lv := range labels {
			if c.labels[idx] != lv {
				match = false
				break
			}
		}
		if match {
			count++
		}
	}
	return count
}

// callsWithPrefix counts calls whose label slice starts with the given prefix.
func (f *fakeMetricCounter) callsWithPrefix(prefix ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, c := range f.calls {
		if len(c.labels) < len(prefix) {
			continue
		}
		match := true
		for idx, lv := range prefix {
			if c.labels[idx] != lv {
				match = false
				break
			}
		}
		if match {
			count++
		}
	}
	return count
}

type fakeMetricGauge struct {
	calls []labeledCall
	mu    sync.Mutex
}

func (f *fakeMetricGauge) Set(value float64, labels ...string) {
	cp := make([]string, len(labels))
	copy(cp, labels)
	f.mu.Lock()
	f.calls = append(f.calls, labeledCall{delta: value, labels: cp})
	f.mu.Unlock()
}

func (f *fakeMetricGauge) Add(delta float64, labels ...string) {
	f.mu.Lock()
	f.calls = append(f.calls, labeledCall{delta: delta, labels: labels})
	f.mu.Unlock()
}

func (f *fakeMetricGauge) Delete(_ ...string) bool { return true }

func (f *fakeMetricGauge) lastValue(group string) (float64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for idx := len(f.calls) - 1; idx >= 0; idx-- {
		c := f.calls[idx]
		if len(c.labels) > 0 && c.labels[0] == group {
			return c.delta, true
		}
	}
	return 0, false
}

// fakeMetricFactory creates counters/gauges that record calls for assertion.
type fakeMetricFactory struct {
	counters map[string]*fakeMetricCounter
	gauges   map[string]*fakeMetricGauge
	mu       sync.Mutex
}

func newFakeMetricFactory() *fakeMetricFactory {
	return &fakeMetricFactory{
		counters: make(map[string]*fakeMetricCounter),
		gauges:   make(map[string]*fakeMetricGauge),
	}
}

func (f *fakeMetricFactory) NewCounter(name string, _ ...string) meter.Counter {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Memoize by name so repeated NewCounter for the same series name returns the
	// same handle, mirroring the real registry: two groups sharing a plugin metric
	// name wrap ONE physical series and disambiguate via their prepended group
	// label value. This is what the cross-group attribution assertion depends on.
	if c, ok := f.counters[name]; ok {
		return c
	}
	c := &fakeMetricCounter{}
	f.counters[name] = c
	return c
}

func (f *fakeMetricFactory) NewGauge(name string, _ ...string) meter.Gauge {
	f.mu.Lock()
	defer f.mu.Unlock()
	if g, ok := f.gauges[name]; ok {
		return g
	}
	g := &fakeMetricGauge{}
	f.gauges[name] = g
	return g
}

func (f *fakeMetricFactory) NewHistogram(_ string, _ meter.Buckets, _ ...string) meter.Histogram {
	return observability.BypassRegistry.With(pipelineScope).NewHistogram("", meter.Buckets{})
}

func (f *fakeMetricFactory) Close() {}

// counter/gauge accessors.
func (f *fakeMetricFactory) counter(name string) *fakeMetricCounter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counters[name]
}

func (f *fakeMetricFactory) gauge(name string) *fakeMetricGauge {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gauges[name]
}

// Helpers.

// makeMeteredSchemaRepo builds a ROLE_DATA schemaRepo with the supplied factory
// wired into samplerMeter. The Repository field is nil — reconcilePipeline does
// not touch it.
func makeMeteredSchemaRepo(factory observability.Factory) schemaRepo {
	return schemaRepo{
		l:                     logger.GetLogger("pipeline-metrics-test"),
		role:                  databasev1.Role_ROLE_DATA,
		nativePipelineEnabled: true,
		trustedPluginDir:      "/nonexistent",
		samplerMeter:          newSamplerMetrics(factory),
	}
}

// nilMeteredSchemaRepo builds a schemaRepo without a samplerMeter to verify
// nil-safe behavior.
func nilMeteredSchemaRepo() schemaRepo {
	return schemaRepo{
		l:                     logger.GetLogger("pipeline-metrics-test"),
		role:                  databasev1.Role_ROLE_DATA,
		nativePipelineEnabled: true,
		trustedPluginDir:      "/nonexistent",
	}
}

// Tests.

// TestSamplerMetrics_RegisterUpdateRemove asserts that the full register →
// update → remove lifecycle emits the correct labels and counts.
func TestSamplerMetrics_RegisterUpdateRemove(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr := makeMeteredSchemaRepo(factory)

	const group = "metrics-group"
	dummy := &dummySampler{}

	// --- register (no previous set) ---
	replaceSamplersForGroup(group, []namedSampler{{name: "prev", sampler: dummy}})
	// Simulate a successful register: clear first so isUpdate == false.
	removeSamplersForGroup(group)

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: "lss",
				Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{
					Path:       "missing.so",
					Symbol:     "NewSampler",
					AbiVersion: uint32(sdk.ABIVersion),
				}},
			},
		},
	}

	// Load will fail (nonexistent .so) → rejected on register path.
	sr.reconcilePipeline(group, cfg)

	regTotal := factory.counter("sampler_register_total")
	require.NotNil(t, regTotal)
	assert.Equal(t, 1, regTotal.callsWithLabels(group, "rejected"),
		"rejected register must increment sampler_register_total{group,result=rejected}")
	loadFailed := factory.counter("sampler_load_failed")
	require.NotNil(t, loadFailed)
	assert.Equal(t, 1, loadFailed.callsWithPrefix(group, "lss"),
		"sampler_load_failed must be emitted with labels {group, name=lss, reason=...}")

	// --- update path: pre-populate so isUpdate == true, then load fails again ---
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	sr.reconcilePipeline(group, cfg)

	updTotal := factory.counter("sampler_update_total")
	require.NotNil(t, updTotal)
	assert.Equal(t, 1, updTotal.callsWithLabels(group, "rejected"),
		"rejected update must increment sampler_update_total{group,result=rejected}")

	// --- remove via nil config ---
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	sr.reconcilePipeline(group, nil)

	rmTotal := factory.counter("sampler_remove_total")
	require.NotNil(t, rmTotal)
	assert.Equal(t, 1, rmTotal.callsWithLabels(group),
		"nil config must increment sampler_remove_total{group}")

	activeGauge := factory.gauge("sampler_active_count")
	require.NotNil(t, activeGauge)
	lastVal, ok := activeGauge.lastValue(group)
	require.True(t, ok, "sampler_active_count must have been set at least once")
	assert.Equal(t, float64(0), lastVal, "sampler_active_count must be 0 after remove")
}

// TestSamplerMetrics_SuccessUpdateAndIdempotentSkip exercises the success metric
// path via a real config change and verifies the idempotent-skip: an
// empty-plugins config on an empty registry is a no-op (no metric churn), while a
// transition from a previously registered set to the empty set is a genuine
// update that emits update-success and sets active_count=0. A second identical
// empty reconcile is then skipped.
func TestSamplerMetrics_SuccessUpdateAndIdempotentSkip(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr := makeMeteredSchemaRepo(factory)

	const group = "success-group"

	// Empty-plugins config on an empty registry: desired == current == [] →
	// idempotent skip. No register/update/active metric is emitted.
	emptyCfg := &commonv1.TracePipelineConfig{Enabled: true}
	sr.reconcilePipeline(group, emptyCfg)

	regTotal := factory.counter("sampler_register_total")
	if regTotal != nil {
		assert.Equal(t, 0, regTotal.callsWithLabels(group, "success"),
			"empty-plugins config on an empty registry must be idempotently skipped")
	}

	// Seed a previously registered set, then reconcile to the empty-plugins
	// config. desired ([]) != current (one sampler) → genuine update → success.
	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	sr.reconcilePipeline(group, emptyCfg)

	updTotal := factory.counter("sampler_update_total")
	require.NotNil(t, updTotal)
	assert.Equal(t, 1, updTotal.callsWithLabels(group, "success"),
		"a set→empty transition must emit sampler_update_total{group,result=success}")

	activeGauge := factory.gauge("sampler_active_count")
	require.NotNil(t, activeGauge)
	lastVal, ok := activeGauge.lastValue(group)
	require.True(t, ok)
	assert.Equal(t, float64(0), lastVal, "active_count must be 0 after clearing to the empty set")

	// Second identical empty reconcile: desired == current == [] → skipped, so
	// update-success stays at exactly 1.
	sr.reconcilePipeline(group, emptyCfg)
	assert.Equal(t, 1, updTotal.callsWithLabels(group, "success"),
		"a redundant identical empty reconcile must be idempotently skipped")
}

// TestSamplerMetrics_OnDeleteKindGroup verifies that the OnDelete KindGroup
// path (simulated directly) emits sampler_remove_total and sets active_count=0.
func TestSamplerMetrics_OnDeleteKindGroup(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr := makeMeteredSchemaRepo(factory)

	const group = "delete-metrics-group"
	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	// Simulate OnDelete KindGroup gate.
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
	}
	if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled {
		removeSamplersForGroup(g.Metadata.Name)
		sr.samplerMeter.setActiveCount(g.Metadata.Name, 0)
		sr.samplerMeter.incRemoveTotal(g.Metadata.Name)
	}

	rmTotal := factory.counter("sampler_remove_total")
	require.NotNil(t, rmTotal)
	assert.Equal(t, 1, rmTotal.callsWithLabels(group),
		"OnDelete KindGroup must increment sampler_remove_total{group}")

	activeGauge := factory.gauge("sampler_active_count")
	require.NotNil(t, activeGauge)
	lastVal, ok := activeGauge.lastValue(group)
	require.True(t, ok)
	assert.Equal(t, float64(0), lastVal, "sampler_active_count must be 0 after OnDelete")
}

// TestSamplerMetrics_NilSafe verifies that a nil samplerMeter on schemaRepo
// does not panic during reconcilePipeline or the OnDelete simulation.
func TestSamplerMetrics_NilSafe(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	sr := nilMeteredSchemaRepo()
	require.Nil(t, sr.samplerMeter)

	const group = "nil-safe-group"
	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})

	require.NotPanics(t, func() {
		sr.reconcilePipeline(group, nil)
	}, "nil samplerMeter must not panic on reconcilePipeline(nil)")

	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotPanics(t, func() {
		removeSamplersForGroup(group)
		sr.samplerMeter.setActiveCount(group, 0)
		sr.samplerMeter.incRemoveTotal(group)
	}, "nil samplerMeter must not panic on OnDelete path")
}
