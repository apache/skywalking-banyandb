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

//go:build trace_pipeline

package trace

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// telemetryPluginPkgPath is the path to the telemetrysampler plugin source
// relative to this file's directory (banyand/trace).
const telemetryPluginPkgPath = "../../test/plugins/_telemetrysampler"

// captureStderr redirects the process-level stderr file descriptor around fn,
// capturing all bytes written to fd 2 during the call. It reads the pipe after
// closing the write end, so small output (a few log lines) is collected without
// risk of deadlock.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, pipeErr := os.Pipe()
	require.NoError(t, pipeErr, "os.Pipe for stderr capture must succeed")

	savedFd, dupErr := syscall.Dup(int(os.Stderr.Fd()))
	require.NoError(t, dupErr, "syscall.Dup of stderr must succeed")

	dup2Err := syscall.Dup2(int(w.Fd()), int(os.Stderr.Fd()))
	require.NoError(t, dup2Err, "syscall.Dup2 redirecting stderr to pipe must succeed")

	fn()

	// Close the write end so the reader sees EOF.
	w.Close()

	// Restore the original stderr fd before reading (keeps test output visible).
	_ = syscall.Dup2(savedFd, int(os.Stderr.Fd()))
	_ = syscall.Close(savedFd)

	captured, readErr := io.ReadAll(r)
	r.Close()
	require.NoError(t, readErr, "reading captured stderr must succeed")

	return string(captured)
}

// labelValue returns the value of label name in m, or "" if not found.
func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// TestPluginTelemetry_RealSO_MetricsAndLogging loads a REAL .so built from
// ../../test/plugins/_telemetrysampler and verifies that:
//   - reconcilePipeline wires the plugin into the real prom-backed meter;
//   - one Decide call on a 3-trace batch increments the
//     banyandb_trace_pipeline_plugin_decisions{verdict="keep"} counter by 3;
//   - the plugin emits a structured log line through the global logger (→ stderr).
//
// The test skips when the race detector is active (a non-race .so cannot be
// loaded into a race host), when CGO is unavailable, or when the toolchain
// versions diverge.
func TestPluginTelemetry_RealSO_MetricsAndLogging(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("TestPluginTelemetry_RealSO_MetricsAndLogging skipped: race detector active; " +
			"a non-race .so cannot be loaded into a race-instrumented host")
	}

	resetRegistries()
	defer resetRegistries()

	// newRealSOSchemaRepo handles build, probe, and skip-on-failure.
	// All real-.so tests share a single .so (see newRealSOSchemaRepo).
	sr, soName, reg := newRealSOSchemaRepo(t)

	const group = "realso-group"
	const pluginName = "tel"

	// logEvery=100 (the plugin default): batchN==1 satisfies 1%100==1 → logs on
	// the very first batch. logEvery=1 would give 1%1==0 which never matches.
	cfgStruct, structErr := structpb.NewStruct(map[string]any{"logEvery": float64(100)})
	require.NoError(t, structErr, "structpb.NewStruct for plugin config must succeed")

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: pluginName,
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soName,
						Symbol:     "NewSampler",
						AbiVersion: uint32(sdk.ABIVersion),
						Config:     cfgStruct,
					},
				},
			},
		},
	}

	sr.reconcilePipeline(group, cfg)

	samplers := lookupSamplers(group)
	require.NotEmpty(t, samplers, "reconcilePipeline must register at least one sampler for group %q", group)

	// Invoke Decide while capturing stderr to collect the structured log line.
	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 3)}
	var verdict sdk.Verdict
	var decideErr error
	captured := captureStderr(t, func() {
		verdict, decideErr = samplers[0].Decide(batch)
	})

	require.NoError(t, decideErr, "Decide must not return an error")
	require.Len(t, verdict.Keep, 3, "Decide must return a Keep slice of length 3")
	for idx, keep := range verdict.Keep {
		assert.True(t, keep, "verdict.Keep[%d] must be true (sampler keeps all traces)", idx)
	}

	// --- Metrics assertion ---
	// The host meter adapter names the metric "plugin_decisions"; prom.NewProvider
	// prefixes with the pipeline scope namespace producing:
	//   banyandb_trace_pipeline_plugin_decisions{group, plugin_name, verdict}
	mfs, gatherErr := reg.Gather()
	require.NoError(t, gatherErr, "prometheus registry Gather must succeed")

	const wantFamily = "banyandb_trace_pipeline_plugin_decisions"
	var foundFamily *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == wantFamily {
			foundFamily = mf
			break
		}
	}
	require.NotNilf(t, foundFamily,
		"metric family %q not found in gathered families; got: %v",
		wantFamily, metricFamilyNames(mfs))

	var foundMetric *dto.Metric
	for _, m := range foundFamily.GetMetric() {
		if labelValue(m, "group") == group &&
			labelValue(m, "plugin_name") == pluginName &&
			labelValue(m, "verdict") == "keep" {
			foundMetric = m
			break
		}
	}
	require.NotNilf(t, foundMetric,
		"metric series {group=%q, plugin_name=%q, verdict=%q} not found in family %q",
		group, pluginName, "keep", wantFamily)

	assert.Equal(t, float64(3), foundMetric.GetCounter().GetValue(),
		"plugin_decisions counter must equal the number of traces in the batch (3)")

	// --- Logging assertion ---
	// The host logger adapter names the module TRACE.PLUGIN.<UPPER(GROUP)>.<UPPER(PLUGIN)>.
	wantModule := "TRACE.PLUGIN." + strings.ToUpper(group) + "." + strings.ToUpper(pluginName)
	wantMsg := "batch decided"
	assert.True(t, strings.Contains(captured, wantModule),
		"captured stderr must contain the plugin logger module name %q; got:\n%s", wantModule, captured)
	assert.True(t, strings.Contains(captured, wantMsg),
		"captured stderr must contain the log message %q; got:\n%s", wantMsg, captured)
}

// metricFamilyNames returns the names of all metric families for use in
// diagnostic failure messages.
func metricFamilyNames(mfs []*dto.MetricFamily) []string {
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	return names
}

// sharedSO holds the result of the once-per-process .so build and probe.
// Go deduplicates plugins by their internal package path (not by filesystem
// path), so all real-.so tests must share a single .so file: a second
// plugin.Open on a different filesystem path but the same internal plugin
// path causes a "previous failure" error in the Go runtime.
var sharedSO struct {
	dir        string
	soPath     string
	skipReason string // non-empty → tests must skip
	sync.Once
}

// initSharedSO builds the telemetrysampler .so exactly once into a
// process-scoped temp directory and probes that the host can load it.
// Results are stored in sharedSO; the caller must check sharedSO.skipReason.
func initSharedSO() {
	// os.MkdirTemp creates a directory that persists for the process lifetime
	// (not cleaned by t.Cleanup), which is intentional: the .so must outlive
	// any individual test's cleanup to prevent the path from being invalidated
	// while the process-level plugin registration still references it.
	dir, mkErr := os.MkdirTemp("", "realso-shared-*")
	if mkErr != nil {
		sharedSO.skipReason = "initSharedSO: os.MkdirTemp failed: " + mkErr.Error()
		return
	}
	sharedSO.dir = dir

	goExe, lookErr := exec.LookPath("go")
	if lookErr != nil {
		sharedSO.skipReason = "initSharedSO: 'go' binary not found in PATH"
		return
	}
	soPath := filepath.Join(dir, "telemetrysampler.so")

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		sharedSO.skipReason = "initSharedSO: cannot determine source file path via runtime.Caller"
		return
	}
	absPluginDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), telemetryPluginPkgPath))

	cmd := exec.Command(goExe, "build", "-buildmode=plugin", "-o", soPath, absPluginDir)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, buildErr := cmd.CombinedOutput()
	if buildErr != nil {
		sharedSO.skipReason = "initSharedSO: cannot build .so (CGO may be unavailable): " +
			buildErr.Error() + "\n" + string(out)
		return
	}
	sharedSO.soPath = soPath

	_, probeErr := plugin.Open(soPath)
	if isPluginToolchainMismatch(probeErr) {
		sharedSO.skipReason = "initSharedSO: host cannot load freshly built plugin (toolchain mismatch): " +
			probeErr.Error()
		return
	}
	if probeErr != nil {
		sharedSO.skipReason = "initSharedSO: plugin.Open probe failed: " + probeErr.Error()
	}
}

// newRealSOSchemaRepo is a shared setup helper for real-.so tests: it ensures
// the telemetrysampler .so is built and probed (skipping on CGO unavailability
// or toolchain mismatch), and returns a ready-to-use schemaRepo together with
// the .so file name (for Plugin.Path) and the prometheus registry.
//
// All real-.so tests share a single .so file because Go deduplicates plugins
// by their internal package path: opening the same plugin package from two
// different filesystem paths in the same process causes a "previous failure"
// error on the second open.
func newRealSOSchemaRepo(t *testing.T) (sr schemaRepo, soName string, reg *prometheus.Registry) {
	t.Helper()
	sharedSO.Do(initSharedSO)
	if sharedSO.skipReason != "" {
		t.Skip(sharedSO.skipReason)
	}

	reg = prometheus.NewRegistry()
	promProvider := prom.NewProvider(pipelineScope, reg)
	factory := services.NewFactory(promProvider, nil, nil)

	sr = schemaRepo{
		l:                      logger.GetLogger("trace"),
		role:                   databasev1.Role_ROLE_DATA,
		nativePipelineEnabled:  true,
		trustedPluginDir:       sharedSO.dir,
		samplerMeter:           newSamplerMetrics(factory),
		pluginTelemetryFactory: factory,
	}
	soName = filepath.Base(sharedSO.soPath)
	return sr, soName, reg
}

// faultyPluginPkgPath is the path to the faultysampler plugin source relative to
// this file's directory (banyand/trace). It is a distinct Go package from
// _telemetrysampler, so its .so can be loaded in the same process without the
// Go plugin-dedup "previous failure" error.
const faultyPluginPkgPath = "../../test/plugins/_faultysampler"

// sharedFaultySO holds the result of the once-per-process .so build and probe
// for the _faultysampler plugin. Strings/pointer fields come first so the
// compiler can satisfy fieldalignment before the embedded sync.Once.
var sharedFaultySO struct {
	dir        string
	soPath     string
	skipReason string // non-empty → tests must skip
	sync.Once
}

// initSharedFaultySO builds the faultysampler .so exactly once into a
// process-scoped temp directory and probes that the host can load it.
func initSharedFaultySO() {
	dir, mkErr := os.MkdirTemp("", "realso-faulty-*")
	if mkErr != nil {
		sharedFaultySO.skipReason = "initSharedFaultySO: os.MkdirTemp failed: " + mkErr.Error()
		return
	}
	sharedFaultySO.dir = dir

	goExe, lookErr := exec.LookPath("go")
	if lookErr != nil {
		sharedFaultySO.skipReason = "initSharedFaultySO: 'go' binary not found in PATH"
		return
	}
	soPath := filepath.Join(dir, "faultysampler.so")

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		sharedFaultySO.skipReason = "initSharedFaultySO: cannot determine source file path via runtime.Caller"
		return
	}
	absPluginDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), faultyPluginPkgPath))

	cmd := exec.Command(goExe, "build", "-buildmode=plugin", "-o", soPath, absPluginDir)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, buildErr := cmd.CombinedOutput()
	if buildErr != nil {
		sharedFaultySO.skipReason = "initSharedFaultySO: cannot build .so (CGO may be unavailable): " +
			buildErr.Error() + "\n" + string(out)
		return
	}
	sharedFaultySO.soPath = soPath

	_, probeErr := plugin.Open(soPath)
	if isPluginToolchainMismatch(probeErr) {
		sharedFaultySO.skipReason = "initSharedFaultySO: host cannot load freshly built plugin (toolchain mismatch): " +
			probeErr.Error()
		return
	}
	if probeErr != nil {
		sharedFaultySO.skipReason = "initSharedFaultySO: plugin.Open probe failed: " + probeErr.Error()
	}
}

// newRealSOFaultyRepo sets up the faultysampler .so and returns a schemaRepo,
// the .so base name (for Plugin.Path), and a fresh prometheus.Registry. It
// skips the test when CGO is unavailable or there is a toolchain mismatch.
func newRealSOFaultyRepo(t *testing.T) (sr schemaRepo, soName string, reg *prometheus.Registry) {
	t.Helper()
	sharedFaultySO.Do(initSharedFaultySO)
	if sharedFaultySO.skipReason != "" {
		t.Skip(sharedFaultySO.skipReason)
	}

	reg = prometheus.NewRegistry()
	promProvider := prom.NewProvider(pipelineScope, reg)
	factory := services.NewFactory(promProvider, nil, nil)

	sr = schemaRepo{
		l:                      logger.GetLogger("trace"),
		role:                   databasev1.Role_ROLE_DATA,
		nativePipelineEnabled:  true,
		trustedPluginDir:       sharedFaultySO.dir,
		samplerMeter:           newSamplerMetrics(factory),
		pluginTelemetryFactory: factory,
	}
	soName = filepath.Base(sharedFaultySO.soPath)
	return sr, soName, reg
}

// makeFaultyCfg builds a TracePipelineConfig with one faultysampler plugin entry.
// faults is marshaled into structpb and passed as the plugin config.
func makeFaultyCfg(soName, pluginName string, faults map[string]any) *commonv1.TracePipelineConfig {
	cfgStruct, structErr := structpb.NewStruct(faults)
	if structErr != nil {
		panic("makeFaultyCfg: structpb.NewStruct failed: " + structErr.Error())
	}
	return &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: pluginName,
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soName,
						Symbol:     "NewSampler",
						AbiVersion: uint32(sdk.ABIVersion),
						Config:     cfgStruct,
					},
				},
			},
		},
	}
}

// gatherCounterValue returns the counter value for the first metric in family
// familyName whose labels match all entries in wantLabels, or 0 if not found.
func gatherCounterValue(mfs []*dto.MetricFamily, familyName string, wantLabels map[string]string) float64 {
	for _, mf := range mfs {
		if mf.GetName() != familyName {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for k, v := range wantLabels {
				if labelValue(m, k) != v {
					match = false
					break
				}
			}
			if match {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// countFamilySeriesForGroup counts how many metric series in familyName have a
// "group" label equal to group.
func countFamilySeriesForGroup(mfs []*dto.MetricFamily, familyName, group string) int {
	for _, mf := range mfs {
		if mf.GetName() != familyName {
			continue
		}
		count := 0
		for _, m := range mf.GetMetric() {
			if labelValue(m, "group") == group {
				count++
			}
		}
		return count
	}
	return 0
}

// TestPluginTelemetry_RealSO_FaultInjection loads the _faultysampler .so and
// verifies that the host telemetry bounds hold under adversarial plugin behavior:
//
//   - flood_logs_are_rate_limited: 500 Logger.Info calls per Decide are clamped
//     by the 50/s burst-100 token bucket; logged + dropped == 500.
//   - cardinality_is_capped: 130 Decide calls each registering a unique Counter
//     label value are capped at 100 kept + 1 overflow series; 30 rejected.
//   - usehost_panic_is_isolated: a panic in UseHost is recovered; the sampler is
//     still registered, keeps all traces, and the panic counter is incremented.
func TestPluginTelemetry_RealSO_FaultInjection(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("TestPluginTelemetry_RealSO_FaultInjection skipped: race detector active; " +
			"a non-race .so cannot be loaded into a race-instrumented host")
	}

	resetRegistries()
	defer resetRegistries()

	sr, soName, reg := newRealSOFaultyRepo(t)

	t.Run("flood_logs_are_rate_limited", func(t *testing.T) {
		const group = "fault-flood"
		cfg := makeFaultyCfg(soName, "fault", map[string]any{
			"floodLogs":  true,
			"floodCount": float64(500),
		})
		sr.reconcilePipeline(group, cfg)

		samplers := lookupSamplers(group)
		require.NotEmpty(t, samplers, "reconcilePipeline must register at least one sampler for group %q", group)

		var logged int
		batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 1)}
		captured := captureStderr(t, func() {
			_, decideErr := samplers[0].Decide(batch)
			require.NoError(t, decideErr, "Decide must not return an error")
		})
		logged = strings.Count(captured, "flood log line")

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr, "prometheus registry Gather must succeed")

		dropped := gatherCounterValue(mfs,
			"banyandb_trace_pipeline_plugin_log_dropped_total",
			map[string]string{"group": group, "plugin_name": "fault"})

		assert.Greater(t, logged, 0,
			"at least one log line must pass the rate limiter; got 0")
		assert.Less(t, logged, 500,
			"rate limiter must clamp output below 500; got %d", logged)
		assert.Greater(t, dropped, float64(0),
			"plugin_log_dropped_total must be >0 when 500 lines are emitted at once; got %v", dropped)
		assert.Equal(t, 500, logged+int(dropped),
			"every Info call must either be logged or counted as dropped: logged=%d dropped=%v", logged, dropped)
	})

	t.Run("cardinality_is_capped", func(t *testing.T) {
		const group = "fault-card"
		cfg := makeFaultyCfg(soName, "fault", map[string]any{
			"explodeCardinality": true,
		})
		sr.reconcilePipeline(group, cfg)

		samplers := lookupSamplers(group)
		require.NotEmpty(t, samplers, "reconcilePipeline must register at least one sampler for group %q", group)

		batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 1)}
		for range 130 {
			_, decideErr := samplers[0].Decide(batch)
			require.NoError(t, decideErr, "Decide must not return an error")
		}

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr, "prometheus registry Gather must succeed")

		rejected := gatherCounterValue(mfs,
			"banyandb_trace_pipeline_plugin_telemetry_series_rejected_total",
			map[string]string{"group": group, "plugin_name": "fault"})
		assert.Equal(t, float64(30), rejected,
			"series_rejected must equal 130 calls − 100 cap = 30; got %v", rejected)

		boomSeries := countFamilySeriesForGroup(mfs, "banyandb_trace_pipeline_plugin_boom", group)
		assert.Equal(t, 101, boomSeries,
			"boom family must have 101 series for group %q (100 kept + 1 __overflow__); got %d", group, boomSeries)
	})

	t.Run("usehost_panic_is_isolated", func(t *testing.T) {
		const group = "fault-panic"
		cfg := makeFaultyCfg(soName, "fault", map[string]any{
			"panicInUseHost": true,
		})

		require.NotPanics(t, func() { sr.reconcilePipeline(group, cfg) },
			"reconcilePipeline must not panic when the plugin's UseHost panics")

		samplers := lookupSamplers(group)
		require.NotEmpty(t, samplers,
			"sampler must still be registered after a UseHost panic (fail-open)")

		batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 2)}
		verdict, decideErr := samplers[0].Decide(batch)
		require.NoError(t, decideErr, "Decide must not return an error after a UseHost panic")
		require.Len(t, verdict.Keep, 2, "Decide must return a Keep slice of length 2 (fail-open)")
		for idx, keep := range verdict.Keep {
			assert.True(t, keep, "verdict.Keep[%d] must be true (sampler keeps all traces)", idx)
		}

		mfs, gatherErr := reg.Gather()
		require.NoError(t, gatherErr, "prometheus registry Gather must succeed")

		panicCount := gatherCounterValue(mfs,
			"banyandb_trace_pipeline_plugin_telemetry_panic_total",
			map[string]string{"group": group, "plugin_name": "fault"})
		assert.Equal(t, float64(1), panicCount,
			"plugin_telemetry_panic_total must equal 1 after one UseHost panic; got %v", panicCount)
	})
}

// makeSamplerCfg builds a TracePipelineConfig with a single telemetrysampler
// plugin entry using the given soName, logEvery value, and plugin name.
func makeSamplerCfg(soName, pluginName string, logEvery float64) *commonv1.TracePipelineConfig {
	cfgStruct, structErr := structpb.NewStruct(map[string]any{"logEvery": logEvery})
	if structErr != nil {
		panic("makeSamplerCfg: structpb.NewStruct failed: " + structErr.Error())
	}
	return &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: pluginName,
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soName,
						Symbol:     "NewSampler",
						AbiVersion: uint32(sdk.ABIVersion),
						Config:     cfgStruct,
					},
				},
			},
		},
	}
}

// TestPluginTelemetry_RealSO_ConfigParamLogEvery proves that the logEvery config
// parameter reaches the real .so and changes the log-emission frequency.
//
// The plugin logs "batch decided" when batchN % logEvery == 1 (batchN starts at 1
// and increments per Decide call on the instance).
//   - Sub-case A: logEvery=2 → batches 1,3,5 satisfy n%2==1 → 3 log lines in 5 calls.
//   - Sub-case B: logEvery=1000 → only batch 1 satisfies n%1000==1 → 1 log line in 5 calls.
func TestPluginTelemetry_RealSO_ConfigParamLogEvery(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("TestPluginTelemetry_RealSO_ConfigParamLogEvery skipped: race detector active; " +
			"a non-race .so cannot be loaded into a race-instrumented host")
	}

	resetRegistries()
	defer resetRegistries()

	sr, soName, _ := newRealSOSchemaRepo(t)

	const pluginName = "tel"

	// --- Sub-case A: logEvery=2 ---
	// batchN%2==1 for batchN in {1,3,5}: expect exactly 3 "batch decided" lines.
	const groupA = "cfgparam-a"
	cfgA := makeSamplerCfg(soName, pluginName, float64(2))
	sr.reconcilePipeline(groupA, cfgA)

	samplersA := lookupSamplers(groupA)
	require.NotEmpty(t, samplersA, "reconcilePipeline must register at least one sampler for group %q", groupA)

	batchSingle := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 1)}
	var countA int
	capturedA := captureStderr(t, func() {
		for range 5 {
			_, decideErr := samplersA[0].Decide(batchSingle)
			require.NoError(t, decideErr, "Decide must not error (group %q)", groupA)
		}
	})
	countA = strings.Count(capturedA, "batch decided")

	// --- Sub-case B: logEvery=1000 ---
	// batchN%1000==1 only for batchN==1: expect exactly 1 "batch decided" line.
	const groupB = "cfgparam-b"
	cfgB := makeSamplerCfg(soName, pluginName, float64(1000))
	sr.reconcilePipeline(groupB, cfgB)

	samplersB := lookupSamplers(groupB)
	require.NotEmpty(t, samplersB, "reconcilePipeline must register at least one sampler for group %q", groupB)

	var countB int
	capturedB := captureStderr(t, func() {
		for range 5 {
			_, decideErr := samplersB[0].Decide(batchSingle)
			require.NoError(t, decideErr, "Decide must not error (group %q)", groupB)
		}
	})
	countB = strings.Count(capturedB, "batch decided")

	// logEvery=2: batches 1,3,5 satisfy batchN%2==1 → 3 log lines.
	assert.Equal(t, 3, countA,
		"logEvery=2: expected 3 log lines (batches 1,3,5 satisfy batchN%%2==1); got %d\nstderr:\n%s", countA, capturedA)
	// logEvery=1000: only batch 1 satisfies batchN%1000==1 → 1 log line.
	assert.Equal(t, 1, countB,
		"logEvery=1000: expected 1 log line (only batch 1 satisfies batchN%%1000==1); got %d\nstderr:\n%s", countB, capturedB)
	assert.Greater(t, countA, countB, "logEvery=2 must produce more log lines than logEvery=1000")
}

// TestPluginTelemetry_RealSO_MalformedConfigFailOpen proves that a malformed
// config value (a string where the plugin's NewSampler expects an int64) causes
// the real .so's constructor to return an error, and that the engine handles this
// fail-open: no sampler is registered and the engine is not wedged.
//
// Note: structpb always produces valid JSON, but the TYPE mismatch
// ({"logEvery":"not-a-number"} where the plugin wants int64) causes
// json.Unmarshal into samplerConfig to fail → NewSampler returns
// "telemetrysampler: invalid config JSON" → reconcilePipeline keeps previous
// (empty) set → lookupSamplers returns nil (fail-open: all traces retained).
func TestPluginTelemetry_RealSO_MalformedConfigFailOpen(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("TestPluginTelemetry_RealSO_MalformedConfigFailOpen skipped: race detector active; " +
			"a non-race .so cannot be loaded into a race-instrumented host")
	}

	resetRegistries()
	defer resetRegistries()

	sr, soName, _ := newRealSOSchemaRepo(t)

	const (
		group      = "malformed-cfg"
		pluginName = "tel"
	)

	// Build a config whose logEvery value is a STRING — valid JSON from structpb's
	// perspective, but the plugin's json.Unmarshal into int64 will fail, causing
	// NewSampler to return "telemetrysampler: invalid config JSON".
	badCfgStruct, badStructErr := structpb.NewStruct(map[string]any{"logEvery": "not-a-number"})
	require.NoError(t, badStructErr, "structpb.NewStruct for bad config must succeed (structpb always accepts string values)")

	badCfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{
				Name: pluginName,
				Kind: &commonv1.Plugin_Sampler{
					Sampler: &commonv1.SamplerPlugin{
						Path:       soName,
						Symbol:     "NewSampler",
						AbiVersion: uint32(sdk.ABIVersion),
						Config:     badCfgStruct,
					},
				},
			},
		},
	}

	// The engine must not panic when the plugin constructor returns an error.
	require.NotPanics(t, func() { sr.reconcilePipeline(group, badCfg) })

	// Fail-open: the constructor error leaves the group with no samplers
	// (the previous set was empty), so all traces are retained.
	assert.Empty(t, lookupSamplers(group),
		"lookupSamplers must be empty after a constructor-error reconcile (fail-open path)")

	// Prove the engine is not wedged: a valid config for the same group must
	// register successfully and produce working Decide calls.
	goodCfg := makeSamplerCfg(soName, pluginName, float64(100))
	require.NotPanics(t, func() { sr.reconcilePipeline(group, goodCfg) })

	samplersAfterRecovery := lookupSamplers(group)
	require.NotEmpty(t, samplersAfterRecovery,
		"lookupSamplers must be non-empty after a valid-config reconcile (engine must recover from bad-config)")

	// Verify the recovered sampler works: a 2-trace batch must return keep-all.
	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 2)}
	verdict, decideErr := samplersAfterRecovery[0].Decide(batch)
	require.NoError(t, decideErr, "Decide must not error after recovery")
	require.Len(t, verdict.Keep, 2, "Decide must return a Keep slice of length 2 after recovery")
	for idx, keep := range verdict.Keep {
		assert.True(t, keep, "verdict.Keep[%d] must be true after recovery (sampler keeps all traces)", idx)
	}
}
