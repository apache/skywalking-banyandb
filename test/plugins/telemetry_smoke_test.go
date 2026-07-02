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

// Package plugins_test is a build/load smoke test for the reference telemetry
// plugins.  It compiles each plugin as a .so in a temporary directory, opens
// it with plugin.Open, and verifies the exported symbols and the sdk.HostAware
// type assertion.  The test is gated behind the trace_pipeline build tag so it
// only runs when the C toolchain is present (the same gate used by the
// integration suite and the Makefile target).
//
// NOTE: this test must NOT be run with -race.  A race-instrumented host cannot
// load a .so built without -race; the make target (test-trace-pipeline) omits
// -race for exactly this reason.  See the comment above PLUGIN_OUTPUT_DIR in
// the root Makefile for the full rationale.
package plugins_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// isToolchainMismatch reports whether err is the Go runtime's rejection of a
// .so whose build context differs from the host's (compiler version skew).
// plugin.Open surfaces this as "different version of package ...".
func isToolchainMismatch(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "different version of package") ||
		strings.Contains(msg, "plugin was built with a different version")
}

// buildPlugin compiles the plugin at srcDir into outDir/<name>.so and returns
// the .so path.  The test is skipped if the build fails (CGO unavailable, no
// C toolchain, or `go` not found in PATH).
func buildPlugin(t *testing.T, outDir, name, srcDir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Go plugins are not supported on Windows")
	}

	goExe, lookErr := exec.LookPath("go")
	if lookErr != nil {
		t.Skip("telemetry plugin smoke test skipped: 'go' binary not found in PATH")
	}

	// Resolve the plugin source directory from this file's location so the path
	// is correct regardless of the working directory when tests are run.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("telemetry plugin smoke test skipped: cannot determine source file path via runtime.Caller")
	}
	absPluginDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), srcDir))

	soPath := filepath.Join(outDir, name+".so")

	// Do NOT pass -trimpath or -race: go test is not built with -trimpath, and
	// plugin.Open requires the host and plugin to share the same package path
	// encoding.  This test is excluded from -race runs by the trace_pipeline
	// build tag and the Makefile comment; do not add -race here.
	cmd := exec.Command(goExe, "build", "-buildmode=plugin", "-o", soPath, absPluginDir)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, buildErr := cmd.CombinedOutput()
	if buildErr != nil {
		t.Skipf("telemetry plugin smoke test skipped: cannot build %s.so (CGO may be unavailable): %v\n%s",
			name, buildErr, out)
	}
	return soPath
}

// openPlugin opens soPath and skips the test on a toolchain mismatch.
func openPlugin(t *testing.T, soPath string) *plugin.Plugin {
	t.Helper()
	p, openErr := plugin.Open(soPath)
	if isToolchainMismatch(openErr) {
		t.Skipf("telemetry plugin smoke test skipped: host cannot load a freshly built plugin (toolchain mismatch): %v", openErr)
	}
	require.NoError(t, openErr, "plugin.Open(%s)", soPath)
	return p
}

// TestTelemetrySamplerSmoke builds and loads _telemetrysampler, then checks
// ABIVersion, NewSampler, and the sdk.HostAware type assertion.
func TestTelemetrySamplerSmoke(t *testing.T) {
	tmpDir, mkdirErr := os.MkdirTemp("", "telemetrysampler-smoke-*")
	require.NoError(t, mkdirErr)
	defer os.RemoveAll(tmpDir)

	soPath := buildPlugin(t, tmpDir, "telemetrysampler", "_telemetrysampler")
	p := openPlugin(t, soPath)

	// ABIVersion must equal sdk.ABIVersion.
	abiSym, lookErr := p.Lookup("ABIVersion")
	require.NoError(t, lookErr, "ABIVersion symbol must be exported")
	abiPtr, ok := abiSym.(*int)
	require.True(t, ok, "ABIVersion must be *int, got %T", abiSym)
	assert.Equal(t, sdk.ABIVersion, *abiPtr, "ABIVersion mismatch")

	// NewSampler must be present and callable.
	ctorSym, lookErr := p.Lookup("NewSampler")
	require.NoError(t, lookErr, "NewSampler symbol must be exported")
	ctor, ok := ctorSym.(func([]byte) (sdk.Sampler, error))
	require.True(t, ok, "NewSampler must have signature func([]byte)(sdk.Sampler,error), got %T", ctorSym)

	sampler, ctorErr := ctor(nil)
	require.NoError(t, ctorErr, "NewSampler(nil) must succeed")
	require.NotNil(t, sampler)

	// The sampler must implement sdk.HostAware.
	_, isHostAware := sampler.(sdk.HostAware)
	assert.True(t, isHostAware, "telemetrysampler must implement sdk.HostAware")
}

// TestFaultySamplerSmoke builds and loads _faultysampler, then checks
// ABIVersion, NewSampler, and the sdk.HostAware type assertion.
func TestFaultySamplerSmoke(t *testing.T) {
	tmpDir, mkdirErr := os.MkdirTemp("", "faultysampler-smoke-*")
	require.NoError(t, mkdirErr)
	defer os.RemoveAll(tmpDir)

	soPath := buildPlugin(t, tmpDir, "faultysampler", "_faultysampler")
	p := openPlugin(t, soPath)

	// ABIVersion must equal sdk.ABIVersion.
	abiSym, lookErr := p.Lookup("ABIVersion")
	require.NoError(t, lookErr, "ABIVersion symbol must be exported")
	abiPtr, ok := abiSym.(*int)
	require.True(t, ok, "ABIVersion must be *int, got %T", abiSym)
	assert.Equal(t, sdk.ABIVersion, *abiPtr, "ABIVersion mismatch")

	// NewSampler must be present and callable.
	ctorSym, lookErr := p.Lookup("NewSampler")
	require.NoError(t, lookErr, "NewSampler symbol must be exported")
	ctor, ok := ctorSym.(func([]byte) (sdk.Sampler, error))
	require.True(t, ok, "NewSampler must have signature func([]byte)(sdk.Sampler,error), got %T", ctorSym)

	sampler, ctorErr := ctor(nil)
	require.NoError(t, ctorErr, "NewSampler(nil) must succeed")
	require.NotNil(t, sampler)

	// The sampler must implement sdk.HostAware.
	_, isHostAware := sampler.(sdk.HostAware)
	assert.True(t, isHostAware, "faultysampler must implement sdk.HostAware")
}
