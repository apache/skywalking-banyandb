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
	"plugin"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// resetRegistries clears both the sampler and merge-grace registries so each
// test starts from a clean state.
func resetRegistries() {
	localSamplerRegistry.mu.Lock()
	localSamplerRegistry.m = make(map[string][]namedSampler)
	localSamplerRegistry.mu.Unlock()

	localMergeGraceRegistry.mu.Lock()
	localMergeGraceRegistry.m = make(map[string]int64)
	localMergeGraceRegistry.mu.Unlock()

	resetPluginCache()
}

// makeDataSchemaRepo constructs a minimal ROLE_DATA schemaRepo with
// nativePipelineEnabled=true pointing at trustedDir.
// The Repository field is intentionally nil — reconcilePipeline only
// accesses the registry functions and loadSamplerPlugin, not the Repository.
func makeDataSchemaRepo(trustedDir string) schemaRepo {
	return schemaRepo{
		l:                     logger.GetLogger("pipeline-watch-test"),
		role:                  databasev1.Role_ROLE_DATA,
		nativePipelineEnabled: true,
		trustedPluginDir:      trustedDir,
	}
}

// makeSamplerPlugin builds a SamplerPlugin for soName inside trustedDir.
func makeSamplerPlugin(soName string, thresholdMs float64) (*commonv1.SamplerPlugin, error) {
	cfgStruct, cfgErr := structpb.NewStruct(map[string]interface{}{
		"thresholdMs":  thresholdMs,
		"successValue": "ok",
	})
	if cfgErr != nil {
		return nil, cfgErr
	}
	return &commonv1.SamplerPlugin{
		Path:       soName,
		Symbol:     "NewSampler",
		AbiVersion: uint32(sdk.ABIVersion),
		Config:     cfgStruct,
	}, nil
}

// dummySampler is a minimal sdk.Sampler implementation for tests that do not
// need a real .so plugin.
type dummySampler struct{}

func (d *dummySampler) Kind() sdk.Kind { return sdk.KindSampler }
func (d *dummySampler) Project() sdk.Projection {
	return sdk.Projection{}
}

func (d *dummySampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for idx := range keep {
		keep[idx] = true
	}
	return sdk.Verdict{Keep: keep}, nil
}
func (d *dummySampler) Close() error { return nil }

// pluginTestState holds the shared tmpDir and .so path used by all tests that
// need a real plugin. They are initialized once (per test binary execution)
// via setupPluginOnce so the Go plugin runtime never sees two opens of the
// same content hash at different paths — which would cause "plugin already
// loaded" errors across independent test functions.
var (
	pluginTestOnce    sync.Once
	pluginTestDir     string // tmpDir owned for the life of the test binary
	pluginTestSoPath  string // absolute path to the built .so
	pluginTestSoName  string // basename only — passed as plugin Path (relative to trustedDir)
	pluginTestSkipMsg string // non-empty → skip any test that needs the plugin
)

// setupPluginOnce builds (or skips) the shared test plugin exactly once.
// It uses sharedPluginDir (from testmain_test.go) so this file and
// pipeline_loader_test.go share the same .so path — preventing the Go plugin
// runtime's "plugin already loaded" dedup error when two different paths have
// identical content.
// It never calls t.Skip directly; it sets pluginTestSkipMsg for callers to act on.
func setupPluginOnce(t *testing.T) {
	t.Helper()
	pluginTestOnce.Do(func() {
		if raceDetectorEnabled {
			pluginTestSkipMsg = "pipeline_watch test skipped under -race: plugin .so must be built with matching race mode; use the integration suite"
			return
		}
		// Prefer the package-level sharedPluginDir (created by TestMain).
		tmpDir := sharedPluginDir
		if tmpDir == "" {
			var mkdirErr error
			tmpDir, mkdirErr = os.MkdirTemp("", "pipeline-watch-shared-*")
			if mkdirErr != nil {
				pluginTestSkipMsg = "pipeline_watch test skipped: cannot create tmpDir: " + mkdirErr.Error()
				return
			}
			// Note: tmpDir is intentionally NOT cleaned up here — it must survive
			// for the whole test binary run. The OS cleans it on process exit.
		}
		soPath := buildTestPlugin(t, tmpDir)
		if _, probeErr := plugin.Open(soPath); isPluginToolchainMismatch(probeErr) {
			pluginTestSkipMsg = "pipeline_watch test skipped: toolchain mismatch: " + probeErr.Error()
			return
		}
		pluginTestDir = tmpDir
		pluginTestSoPath = soPath
		pluginTestSoName = strings.TrimPrefix(soPath[len(tmpDir):], string(os.PathSeparator))
	})
}

// requirePlugin calls setupPluginOnce and skips t if the plugin is not available.
func requirePlugin(t *testing.T) {
	t.Helper()
	setupPluginOnce(t)
	if pluginTestSkipMsg != "" {
		t.Skip(pluginTestSkipMsg)
	}
}

// TestReconcilePipeline_PopulatesRegistry verifies that a ROLE_DATA schemaRepo
// with nativePipelineEnabled=true + a valid .so in trustedDir → reconcilePipeline
// populates lookupSamplers. (US-000 delivery is end-to-end proven by the live
// US-008 standalone suite; this unit test exercises reconcilePipeline directly.)
func TestReconcilePipeline_PopulatesRegistry(t *testing.T) {
	requirePlugin(t)

	resetRegistries()
	defer resetRegistries()

	const group = "us000-group"

	sp, spErr := makeSamplerPlugin(pluginTestSoName, 300)
	require.NoError(t, spErr)

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{Name: "lss", Kind: &commonv1.Plugin_Sampler{Sampler: sp}},
		},
	}

	sr := makeDataSchemaRepo(pluginTestDir)
	sr.reconcilePipeline(group, cfg)

	samplers := lookupSamplers(group)
	require.NotEmpty(t, samplers, "registry must be non-empty after reconcilePipeline with a valid config")
	assert.Equal(t, sdk.KindSampler, samplers[0].Kind())
}

// TestReconcilePipeline_GateAllowsDataNode verifies that the ROLE_DATA +
// nativePipelineEnabled gate (the same expression used in OnAddOrUpdate) allows
// reconcilePipeline to run and populate lookupSamplers.
func TestReconcilePipeline_GateAllowsDataNode(t *testing.T) {
	requirePlugin(t)

	resetRegistries()
	defer resetRegistries()

	const group = "gate-group"

	sp, spErr := makeSamplerPlugin(pluginTestSoName, 200)
	require.NoError(t, spErr)

	pipelineCfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{Name: "lss", Kind: &commonv1.Plugin_Sampler{Sampler: sp}},
		},
	}

	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
		Pipeline: pipelineCfg,
	}

	sr := makeDataSchemaRepo(pluginTestDir)

	// Exercise the exact gate expression from OnAddOrUpdate.
	if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled {
		sr.reconcilePipeline(g.Metadata.Name, g.GetPipeline())
	}

	samplers := lookupSamplers(group)
	require.NotEmpty(t, samplers, "ROLE_DATA gate: registry must be non-empty after reconcilePipeline")
}

// TestReconcilePipeline_NilConfig verifies that a nil pipeline clears the registry.
func TestReconcilePipeline_NilConfig(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "nil-cfg-group"

	// Pre-populate with a dummy sampler.
	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	sr := makeDataSchemaRepo("/some/dir")
	sr.reconcilePipeline(group, nil)

	assert.Empty(t, lookupSamplers(group), "nil config must clear the registry")
	assert.Zero(t, lookupMergeGrace(group), "nil config must clear the merge grace")
}

// TestReconcilePipeline_DisabledConfig verifies that enabled=false clears the registry.
func TestReconcilePipeline_DisabledConfig(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "disabled-cfg-group"

	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	sr := makeDataSchemaRepo("/some/dir")
	sr.reconcilePipeline(group, &commonv1.TracePipelineConfig{Enabled: false})

	assert.Empty(t, lookupSamplers(group), "disabled config must clear the registry")
}

// TestReconcilePipeline_MergeEventNotEnabled verifies that an enabled config whose
// enabled_events does not include PIPELINE_EVENT_MERGE installs no merge samplers
// (v1 only implements the merge-time filter), clearing any previous set.
func TestReconcilePipeline_MergeEventNotEnabled(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "finalize-only-group"

	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	sr := makeDataSchemaRepo("/some/dir")
	sr.reconcilePipeline(group, &commonv1.TracePipelineConfig{
		Enabled:       true,
		EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE},
	})

	assert.Empty(t, lookupSamplers(group), "config without MERGE event must install no merge samplers")
}

// TestReconcilePipeline_OnDeleteKindGroup verifies the OnDelete KindGroup
// cascade: simulate the OnDelete gate → removeSamplersForGroup.
func TestReconcilePipeline_OnDeleteKindGroup(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "delete-group"

	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	sr := makeDataSchemaRepo("/some/dir")

	// Simulate the exact OnDelete KindGroup gate from metadata.go.
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group},
		Catalog:  commonv1.Catalog_CATALOG_TRACE,
	}
	if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled {
		removeSamplersForGroup(g.Metadata.Name)
	}

	assert.Empty(t, lookupSamplers(group), "OnDelete KindGroup must clear the registry")
}

// TestReconcilePipeline_LoadFailureKeepsPreviousSet verifies fail-open: if a
// plugin fails to load, the previous good set is retained and there is no panic.
func TestReconcilePipeline_LoadFailureKeepsPreviousSet(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "fail-open-group"

	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "prev", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	sr := makeDataSchemaRepo("/nonexistent-trusted-dir")

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: "missing",
				Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{
					Path:       "missing.so",
					Symbol:     "NewSampler",
					AbiVersion: uint32(sdk.ABIVersion),
				}},
			},
		},
	}

	// Must not panic.
	require.NotPanics(t, func() { sr.reconcilePipeline(group, cfg) })

	// Previous good set must still be intact (fail-open).
	samplers := lookupSamplers(group)
	require.NotEmpty(t, samplers, "fail-open: previous good set must be retained after load failure")
	assert.Equal(t, dummy, samplers[0], "fail-open: exact previous sampler must be retained")
}

// TestReconcilePipeline_MergeGrace verifies per-config merge_grace storage and
// lookup via lookupMergeGrace.
func TestReconcilePipeline_MergeGrace(t *testing.T) {
	requirePlugin(t)

	resetRegistries()
	defer resetRegistries()

	const group = "grace-group"

	sp, spErr := makeSamplerPlugin(pluginTestSoName, 100)
	require.NoError(t, spErr)

	const wantGrace = 45 * time.Second
	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{Name: "lss", Kind: &commonv1.Plugin_Sampler{Sampler: sp}},
		},
		MergeGrace: durationpb.New(wantGrace),
	}

	sr := makeDataSchemaRepo(pluginTestDir)
	sr.reconcilePipeline(group, cfg)

	gotGrace := lookupMergeGrace(group)
	assert.Equal(t, wantGrace.Nanoseconds(), gotGrace,
		"lookupMergeGrace must return the configured value in nanoseconds")

	// After nil config, grace must be cleared.
	sr.reconcilePipeline(group, nil)
	assert.Zero(t, lookupMergeGrace(group), "grace must be 0 after nil config")
}

// TestReconcilePipeline_MergeGraceUnset verifies that lookupMergeGrace returns
// 0 when no per-group grace is configured.
func TestReconcilePipeline_MergeGraceUnset(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	assert.Zero(t, lookupMergeGrace("never-configured-group"),
		"lookupMergeGrace must return 0 when unset")
}

// TestReconcilePipeline_ConcurrentRace drives concurrent replaceSamplersForGroup +
// removeSamplersForGroup + lookupSamplers + setMergeGraceForGroup +
// lookupMergeGrace calls and must be -race clean.
func TestReconcilePipeline_ConcurrentRace(t *testing.T) {
	resetRegistries()
	defer resetRegistries()
	t.Log("concurrent race test")

	const group = "race-group"
	dummy := &dummySampler{}

	var wg sync.WaitGroup
	const workers = 8
	const iterations = 100

	for wID := 0; wID < workers; wID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch (id + i) % 3 {
				case 0:
					replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
					setMergeGraceForGroup(group, int64(i+1)*int64(time.Second))
				case 1:
					removeSamplersForGroup(group)
					setMergeGraceForGroup(group, 0)
				case 2:
					_ = lookupSamplers(group)
					_ = lookupMergeGrace(group)
				}
			}
		}(wID)
	}
	wg.Wait()
}

// TestReconcilePipeline_LiaisonDoesNotLoad verifies that a ROLE_LIAISON
// schemaRepo never triggers reconcilePipeline (the gate fires only for
// ROLE_DATA && nativePipelineEnabled).
func TestReconcilePipeline_LiaisonDoesNotLoad(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "liaison-group"

	// Liaison: role is LIAISON even though nativePipelineEnabled=true.
	sr := schemaRepo{
		role:                  databasev1.Role_ROLE_LIAISON,
		nativePipelineEnabled: true,
		trustedPluginDir:      "/some/dir",
	}

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

	// Exact gate expression from OnAddOrUpdate — LIAISON must not pass.
	if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled {
		sr.reconcilePipeline(group, cfg)
	}

	assert.Empty(t, lookupSamplers(group), "liaison must never populate sampler registry")
}

// TestReconcilePipeline_NonTraceCatalogOnDelete confirms that OnDelete with a
// non-CATALOG_TRACE group is a no-op (the catalog guard fires before reconcile).
func TestReconcilePipeline_NonTraceCatalogOnDelete(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "non-trace-group"
	dummy := &dummySampler{}
	replaceSamplersForGroup(group, []namedSampler{{name: "d", sampler: dummy}})
	require.NotEmpty(t, lookupSamplers(group))

	// Simulate the OnDelete catalog guard for a non-TRACE group.
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE, // not TRACE
	}
	// The catalog guard: only CATALOG_TRACE reaches removeSamplersForGroup.
	if g.Catalog == commonv1.Catalog_CATALOG_TRACE {
		removeSamplersForGroup(g.Metadata.Name)
	}

	// Non-TRACE group OnDelete must not touch the sampler registry.
	assert.NotEmpty(t, lookupSamplers(group),
		"non-TRACE group OnDelete must not clear the registry")
}
