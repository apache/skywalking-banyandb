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

// buildTelemetryPlugin compiles the telemetrysampler plugin into dir and
// returns the absolute path to the resulting .so. If the build is not possible
// (Windows, CGO unavailable, no C toolchain, or go not found), the test is
// skipped with an actionable message. The .so is built WITHOUT -race so it can
// be loaded by a non-race host.
func buildTelemetryPlugin(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Go plugins are not supported on Windows")
	}

	goExe, lookErr := exec.LookPath("go")
	if lookErr != nil {
		t.Skip("buildTelemetryPlugin skipped: 'go' binary not found in PATH")
	}

	soPath := filepath.Join(dir, "telemetrysampler.so")

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("buildTelemetryPlugin skipped: cannot determine source file path via runtime.Caller")
	}
	absPluginDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), telemetryPluginPkgPath))

	// Do NOT pass -race: this test exercises the non-race .so path explicitly.
	// The raceDetectorEnabled skip-guard at the top of the test prevents a
	// race-instrumented host from attempting to load a non-race .so.
	cmd := exec.Command(goExe, "build", "-buildmode=plugin", "-o", soPath, absPluginDir)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, buildErr := cmd.CombinedOutput()
	if buildErr != nil {
		t.Skipf("buildTelemetryPlugin skipped: cannot build .so (CGO may be unavailable): %v\n%s", buildErr, out)
	}
	return soPath
}

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

	dir := t.TempDir()
	soPath := buildTelemetryPlugin(t, dir)
	soName := filepath.Base(soPath)

	// Probe: even a matching non-race build may be rejected by the runtime when
	// compiler versions differ. Treat that as an environment skip, not a failure.
	if _, probeErr := plugin.Open(soPath); isPluginToolchainMismatch(probeErr) {
		t.Skipf("TestPluginTelemetry_RealSO_MetricsAndLogging skipped: "+
			"host cannot load freshly built plugin (toolchain mismatch): %v", probeErr)
	}

	// Build a real prom-backed observability factory scoped to the pipeline namespace.
	reg := prometheus.NewRegistry()
	promProvider := prom.NewProvider(pipelineScope, reg)
	factory := services.NewFactory(promProvider, nil, nil)

	const group = "realso-group"
	const pluginName = "tel"

	sr := schemaRepo{
		l:                      logger.GetLogger("trace"),
		role:                   databasev1.Role_ROLE_DATA,
		nativePipelineEnabled:  true,
		trustedPluginDir:       dir,
		samplerMeter:           newSamplerMetrics(factory),
		pluginTelemetryFactory: factory,
	}

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
