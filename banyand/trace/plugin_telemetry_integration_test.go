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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// fakeHostAwareSampler is a Go-level sdk.Sampler + sdk.HostAware fake used to
// exercise the cache/keying/bindHost/telemetry-attribution logic under -race
// without a real .so. It is the "Host-caching plugin" the once-only invariant
// hinges on: it caches the injected Host in UseHost and emits attribution
// telemetry through it in Decide.
type fakeHostAwareSampler struct {
	host           sdk.Host
	useHostCount   int64
	panicOnUseHost bool
	panicOnDecide  bool
	mu             sync.Mutex
}

// UseHost caches the engine-owned Host and counts invocations. If configured to
// panic it does so here to prove the engine-side recover still holds.
func (f *fakeHostAwareSampler) UseHost(h sdk.Host) {
	atomic.AddInt64(&f.useHostCount, 1)
	if f.panicOnUseHost {
		panic("fakeHostAwareSampler: UseHost panic")
	}
	f.mu.Lock()
	f.host = h
	f.mu.Unlock()
}

// Decide emits attribution telemetry through the cached Host (proving the emit
// carries this instance's group) and retains the whole batch.
func (f *fakeHostAwareSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	if f.panicOnDecide {
		panic("fakeHostAwareSampler: Decide panic")
	}
	f.mu.Lock()
	h := f.host
	f.mu.Unlock()
	if h != nil {
		h.Meter().Counter("decides").Inc(1)
		h.Logger().Info("decide", "n", len(batch.Traces))
	}
	keep := make([]bool, len(batch.Traces))
	for idx := range keep {
		keep[idx] = true
	}
	return sdk.Verdict{Keep: keep}, nil
}

func (f *fakeHostAwareSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (f *fakeHostAwareSampler) Project() sdk.Projection { return sdk.Projection{} }
func (f *fakeHostAwareSampler) Close() error            { return nil }

// UseHostCount returns the number of UseHost invocations observed so far.
func (f *fakeHostAwareSampler) UseHostCount() int64 { return atomic.LoadInt64(&f.useHostCount) }

// fakeNonHostAware implements only sdk.Sampler (no UseHost) to verify the
// non-HostAware path is left untouched by telemetry wiring.
type fakeNonHostAware struct{}

func (fakeNonHostAware) Kind() sdk.Kind          { return sdk.KindSampler }
func (fakeNonHostAware) Project() sdk.Projection { return sdk.Projection{} }
func (fakeNonHostAware) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for idx := range keep {
		keep[idx] = true
	}
	return sdk.Verdict{Keep: keep}, nil
}
func (fakeNonHostAware) Close() error { return nil }

// makeTelemetrySchemaRepo builds a ROLE_DATA schemaRepo wired to factory for both
// samplerMeter and pluginTelemetryFactory, with trustedPluginDir pointing at a
// real temp dir. It also creates the dummy file named by pluginPath inside that
// dir so the path-within/symlink guards in loadSamplerPlugin pass before the
// newSamplerFromPlugin seam (which the test overrides) is reached.
func makeTelemetrySchemaRepo(t *testing.T, factory *fakeMetricFactory) (schemaRepo, string) {
	t.Helper()
	trustedDir := t.TempDir()
	const pluginPath = "fake.so"
	dummyFile := filepath.Join(trustedDir, pluginPath)
	require.NoError(t, os.WriteFile(dummyFile, []byte("not a real plugin"), 0o600))
	sr := schemaRepo{
		l:                      logger.GetLogger("plugin-telemetry-integration-test"),
		role:                   databasev1.Role_ROLE_DATA,
		nativePipelineEnabled:  true,
		trustedPluginDir:       trustedDir,
		samplerMeter:           newSamplerMetrics(factory),
		pluginTelemetryFactory: factory,
	}
	return sr, pluginPath
}

// makeTelemetryPipelineCfg builds an enabled, merge-event pipeline config with a
// single sampler plugin named pluginName at pluginPath.
func makeTelemetryPipelineCfg(pluginName, pluginPath string) *commonv1.TracePipelineConfig {
	return &commonv1.TracePipelineConfig{
		Enabled:       true,
		EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE},
		Plugins: []*commonv1.Plugin{
			{
				Name: pluginName,
				Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{
					Path:       pluginPath,
					Symbol:     "NewSampler",
					AbiVersion: uint32(sdk.ABIVersion),
				}},
			},
		},
	}
}

// singleTraceBatch returns a one-trace read-only batch for driving Decide.
func singleTraceBatch() *sdk.TraceBatch {
	return &sdk.TraceBatch{Traces: []sdk.TraceBlock{{TraceID: "t"}}}
}

// TestPluginTelemetryIntegration_CrossGroupAttribution asserts that two groups
// sharing the same plugin path+config each construct a DISTINCT HostAware
// instance, each receives exactly one UseHost, and each instance's Decide emits
// telemetry carrying ITS OWN group label — no cross-group label clobber.
func TestPluginTelemetryIntegration_CrossGroupAttribution(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	var constructed []*fakeHostAwareSampler
	var mu sync.Mutex
	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		s := &fakeHostAwareSampler{}
		mu.Lock()
		constructed = append(constructed, s)
		mu.Unlock()
		return s, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	sr.reconcilePipeline("A", cfg)
	sr.reconcilePipeline("B", cfg)

	samplersA := lookupSamplers("A")
	samplersB := lookupSamplers("B")
	require.Len(t, samplersA, 1)
	require.Len(t, samplersB, 1)

	instA, okA := samplersA[0].(*fakeHostAwareSampler)
	instB, okB := samplersB[0].(*fakeHostAwareSampler)
	require.True(t, okA)
	require.True(t, okB)
	assert.NotSame(t, instA, instB, "each group must construct a distinct instance")
	assert.Equal(t, int64(1), instA.UseHostCount(), "group A instance must be hosted exactly once")
	assert.Equal(t, int64(1), instB.UseHostCount(), "group B instance must be hosted exactly once")

	// Drive each group's Decide; the emit must carry that group's label.
	_, decideErrA := samplersA[0].Decide(singleTraceBatch())
	require.NoError(t, decideErrA)
	_, decideErrB := samplersB[0].Decide(singleTraceBatch())
	require.NoError(t, decideErrB)

	decides := factory.counter("plugin_decides")
	require.NotNil(t, decides, "the plugin 'decides' counter must have been created")
	assert.Equal(t, 1, decides.callsWithLabels("A", pluginName),
		"group A Decide must emit decides{group=A,plugin_name=lss}")
	assert.Equal(t, 1, decides.callsWithLabels("B", pluginName),
		"group B Decide must emit decides{group=B,plugin_name=lss}")
}

// TestPluginTelemetryIntegration_ReconcileIdempotentOnceOnly asserts that a
// second identical reconcile reuses the same instance and does NOT re-run
// UseHost or reconstruct (idempotent-skip + once-only).
func TestPluginTelemetryIntegration_ReconcileIdempotentOnceOnly(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	var constructCount int64
	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		atomic.AddInt64(&constructCount, 1)
		return &fakeHostAwareSampler{}, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	sr.reconcilePipeline("A", cfg)
	first := lookupSamplers("A")
	require.Len(t, first, 1)
	inst, ok := first[0].(*fakeHostAwareSampler)
	require.True(t, ok)
	require.Equal(t, int64(1), inst.UseHostCount())
	require.Equal(t, int64(1), atomic.LoadInt64(&constructCount))

	sr.reconcilePipeline("A", cfg)
	second := lookupSamplers("A")
	require.Len(t, second, 1)
	assert.Same(t, inst, second[0], "identical reconcile must reuse the same instance")
	assert.Equal(t, int64(1), inst.UseHostCount(), "once-only: UseHost must not re-run on idempotent reconcile")
	assert.Equal(t, int64(1), atomic.LoadInt64(&constructCount), "idempotent reconcile must not reconstruct")
}

// TestPluginTelemetryIntegration_ConcurrentReconcileDecideRace drives concurrent
// redundant reconcile replays and concurrent Decide calls under the race
// detector. The once-only invariant (UseHostCount == 1) must hold even under
// concurrent replay, with no data race.
func TestPluginTelemetryIntegration_ConcurrentReconcileDecideRace(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		return &fakeHostAwareSampler{}, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	// Seed the instance once so lookups below always find a sampler.
	sr.reconcilePipeline("A", cfg)
	seeded := lookupSamplers("A")
	require.Len(t, seeded, 1)
	inst, ok := seeded[0].(*fakeHostAwareSampler)
	require.True(t, ok)

	var wg sync.WaitGroup
	const reconcilers = 4
	const deciders = 4
	const iterations = 50
	for range reconcilers {
		wg.Go(func() {
			for range iterations {
				sr.reconcilePipeline("A", cfg)
			}
		})
	}
	for range deciders {
		wg.Go(func() {
			for range iterations {
				samplers := lookupSamplers("A")
				if len(samplers) > 0 {
					_, _ = samplers[0].Decide(singleTraceBatch())
				}
			}
		})
	}
	wg.Wait()

	assert.Equal(t, int64(1), inst.UseHostCount(),
		"once-only: UseHost must run exactly once even under concurrent reconcile replay")
	assert.Same(t, inst, lookupSamplers("A")[0], "the same instance must remain registered")
}

// TestPluginTelemetryIntegration_TeardownDeletesOnlyThatGroup asserts that
// removing group A deletes only A's plugin metric series, leaving B intact.
func TestPluginTelemetryIntegration_TeardownDeletesOnlyThatGroup(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		return &fakeHostAwareSampler{}, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	sr.reconcilePipeline("A", cfg)
	sr.reconcilePipeline("B", cfg)

	// Emit a series for each group so there is something to delete.
	_, decideErrA := lookupSamplers("A")[0].Decide(singleTraceBatch())
	require.NoError(t, decideErrA)
	_, decideErrB := lookupSamplers("B")[0].Decide(singleTraceBatch())
	require.NoError(t, decideErrB)

	// Remove group A.
	sr.reconcilePipeline("A", nil)

	decides := factory.counter("plugin_decides")
	require.NotNil(t, decides)
	assert.Equal(t, 1, decides.deletesWithPrefix("A", pluginName),
		"teardown must Delete A's series")
	assert.Equal(t, 0, decides.deletesWithPrefix("B", pluginName),
		"teardown must NOT Delete B's series")

	assert.Empty(t, lookupSamplers("A"), "group A must be removed")
	require.Len(t, lookupSamplers("B"), 1, "group B must remain intact")
}

// TestPluginTelemetryIntegration_NonHostAwareUntouched asserts a non-HostAware
// sampler is registered and runnable with no telemetry, no UseHost, and no panic.
func TestPluginTelemetryIntegration_NonHostAwareUntouched(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "plain"

	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		return fakeNonHostAware{}, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	require.NotPanics(t, func() { sr.reconcilePipeline("A", cfg) })

	samplers := lookupSamplers("A")
	require.Len(t, samplers, 1)
	_, ok := samplers[0].(*fakeHostAwareSampler)
	assert.False(t, ok, "registered sampler must be the non-HostAware fake")
	verdict, decideErr := samplers[0].Decide(singleTraceBatch())
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{true}, verdict.Keep, "non-HostAware sampler must still run")

	// No plugin telemetry series must have been created for the non-HostAware path.
	assert.Nil(t, factory.counter("plugin_decides"),
		"non-HostAware sampler must not create plugin telemetry series")
}

// TestPluginTelemetryIntegration_UseHostPanicIsolation asserts that a UseHost
// panic is isolated: the host does not panic, the sampler is still registered
// and its Decide runs (telemetry disabled), and plugin_telemetry_panic_total is
// incremented for {group, plugin}.
func TestPluginTelemetryIntegration_UseHostPanicIsolation(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		return &fakeHostAwareSampler{panicOnUseHost: true}, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	require.NotPanics(t, func() { sr.reconcilePipeline("A", cfg) },
		"a UseHost panic must not propagate to the host")

	samplers := lookupSamplers("A")
	require.Len(t, samplers, 1, "sampler must still be registered despite the UseHost panic")
	verdict, decideErr := samplers[0].Decide(singleTraceBatch())
	require.NoError(t, decideErr)
	assert.Equal(t, []bool{true}, verdict.Keep, "sampler Decide must still run with telemetry disabled")

	panicCounter := factory.counter("plugin_telemetry_panic_total")
	require.NotNil(t, panicCounter)
	assert.Equal(t, 1, panicCounter.callsWithLabels("A", pluginName),
		"a UseHost panic must increment plugin_telemetry_panic_total{group=A,plugin_name=lss}")
}

// TestPluginTelemetryIntegration_TeardownRetainsAdapterForReuse proves that
// teardownGroupTelemetry RETAINS registry entries so a config-revert that
// re-serves the immortal cached instance can re-emit series and have them
// Deleted again on a subsequent teardown. Under the old delete(m,group)
// behavior the second teardown would find no adapters and the re-created
// series would leak; under the RETAIN behavior the Delete count for the group
// tuple reaches >= 2.
func TestPluginTelemetryIntegration_TeardownRetainsAdapterForReuse(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, _ []byte) (sdk.Sampler, error) {
		return &fakeHostAwareSampler{}, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	cfg := makeTelemetryPipelineCfg(pluginName, pluginPath)
	sr.reconcilePipeline("A", cfg)

	// Step 3: drive Decide to emit the "decides" series.
	samplersFirst := lookupSamplers("A")
	require.Len(t, samplersFirst, 1, "group A must have exactly one sampler after first reconcile")
	inst, ok := samplersFirst[0].(*fakeHostAwareSampler)
	require.True(t, ok)
	_, decideErr := inst.Decide(singleTraceBatch())
	require.NoError(t, decideErr)

	// Step 4: first teardown — must Delete the emitted "A" tuple.
	teardownGroupTelemetry("A")
	decides := factory.counter("plugin_decides")
	require.NotNil(t, decides, "plugin_decides counter must exist after Decide emits it")
	deletesAfterFirst := decides.deletesWithPrefix("A", pluginName)
	assert.GreaterOrEqual(t, deletesAfterFirst, 1,
		"first teardown must Delete the A-scoped series")

	// Step 5: the SAME sampler Decide again (simulates a revert re-serving the
	// immortal cached instance): this re-creates the series in the adapter.
	_, reDecideErr := inst.Decide(singleTraceBatch())
	require.NoError(t, reDecideErr)

	// Step 6: second teardown — the retained adapter must still be findable and
	// Delete the re-created tuple a second time. This is the invariant that would
	// FAIL if teardown used delete(m,group) instead of retaining entries.
	teardownGroupTelemetry("A")
	deletesAfterSecond := decides.deletesWithPrefix("A", pluginName)
	assert.GreaterOrEqual(t, deletesAfterSecond, 2,
		"second teardown must Delete the re-created A-scoped series (adapter retained)")
}

// TestPluginTelemetryIntegration_ConcurrentDistinctConfigRace proves that
// racing reconcile calls for two DISTINCT configs on the same group each
// construct-and-host their respective instance exactly once, even under the
// race detector. The once-only UseHost invariant must hold for both instances
// across concurrent construction.
func TestPluginTelemetryIntegration_ConcurrentDistinctConfigRace(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	factory := newFakeMetricFactory()
	sr, pluginPath := makeTelemetrySchemaRepo(t, factory)
	const pluginName = "lss"

	// instanceByJSON maps cfgJSON (string) to the stable fakeHostAwareSampler
	// allocated on first-miss for that config. Concurrent construction of the
	// same cfgJSON always returns the pointer stored on the first call.
	var instanceMu sync.Mutex
	instanceByJSON := make(map[string]*fakeHostAwareSampler)

	saved := newSamplerFromPlugin
	newSamplerFromPlugin = func(_, _ string, cfgJSON []byte) (sdk.Sampler, error) {
		key := string(cfgJSON)
		instanceMu.Lock()
		defer instanceMu.Unlock()
		if s, exists := instanceByJSON[key]; exists {
			return s, nil
		}
		s := &fakeHostAwareSampler{}
		instanceByJSON[key] = s
		return s, nil
	}
	defer func() { newSamplerFromPlugin = saved }()

	// Build two configs that differ in their structpb config so computeConfigHash
	// returns distinct hashes, causing two distinct pluginCacheKey entries.
	makeCfgWithThreshold := func(thresholdMs float64) *commonv1.TracePipelineConfig {
		cfgStruct, structErr := structpb.NewStruct(map[string]any{
			"thresholdMs": thresholdMs,
		})
		require.NoError(t, structErr)
		return &commonv1.TracePipelineConfig{
			Enabled:       true,
			EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE},
			Plugins: []*commonv1.Plugin{
				{
					Name: pluginName,
					Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{
						Path:       pluginPath,
						Symbol:     "NewSampler",
						AbiVersion: uint32(sdk.ABIVersion),
						Config:     cfgStruct,
					}},
				},
			},
		}
	}
	cfg1 := makeCfgWithThreshold(100)
	cfg2 := makeCfgWithThreshold(200)

	// Launch two goroutine groups racing reconcile for cfg1 and cfg2 on group A,
	// plus a third goroutine that reads and Decides concurrently.
	const iterations = 50
	var wg sync.WaitGroup

	wg.Go(func() {
		for range iterations {
			sr.reconcilePipeline("A", cfg1)
		}
	})

	wg.Go(func() {
		for range iterations {
			sr.reconcilePipeline("A", cfg2)
		}
	})

	wg.Go(func() {
		for range iterations {
			samplers := lookupSamplers("A")
			if len(samplers) > 0 {
				_, _ = samplers[0].Decide(singleTraceBatch())
			}
		}
	})

	wg.Wait()

	// Verify: no data race (the race detector enforces this implicitly), and
	// each distinct constructed instance was hosted exactly once.
	instanceMu.Lock()
	defer instanceMu.Unlock()
	for cfgKey, inst := range instanceByJSON {
		assert.Equal(t, int64(1), inst.UseHostCount(),
			"each distinct constructed instance must be hosted exactly once (cfgKey=%q)", cfgKey)
	}
}
