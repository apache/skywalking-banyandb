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

package sdktest_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// seedPluginPkgPath is the path to the graduated first-party seed plugin,
// relative to this file's directory (pkg/pipeline/sdk/sdktest).
const seedPluginPkgPath = "../../../../plugins/skywalking/latencystatussampler"

// isToolchainMismatch reports whether err is the Go runtime's rejection of a
// .so whose build context differs from the host's (compiler version skew).
func isToolchainMismatch(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "different version of package") ||
		strings.Contains(msg, "plugin was built with a different version")
}

// buildSeedPlugin compiles the latencystatussampler seed plugin into dir and
// returns the absolute path to the resulting .so, skipping the test with an
// actionable message when the build is not possible (no C toolchain, no Go).
func buildSeedPlugin(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Go plugins are not supported on Windows")
	}

	goExe, lookErr := exec.LookPath("go")
	if lookErr != nil {
		t.Skip("LoadSO test skipped: 'go' binary not found in PATH")
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("LoadSO test skipped: cannot determine source file path via runtime.Caller")
	}
	absPluginDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), seedPluginPkgPath))

	soPath := filepath.Join(dir, "latencystatussampler.so")
	// Do NOT pass -trimpath: go test is not built with -trimpath, and
	// plugin.Open requires the host and plugin to share the same package path
	// encoding — mirrors banyand/trace's buildTestPlugin rationale exactly.
	cmd := exec.Command(goExe, "build", "-buildmode=plugin", "-o", soPath, absPluginDir)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, buildErr := cmd.CombinedOutput()
	if buildErr != nil {
		t.Skipf("LoadSO test skipped: cannot build .so (CGO may be unavailable): %v\n%s", buildErr, out)
	}
	return soPath
}

// TestLoadSO_SeedPlugin drives the real, graduated latencystatussampler seed
// plugin (plugins/skywalking/latencystatussampler) through sdktest.LoadSO and
// sdktest.Run — the seed's proof-of-use via the offline dev toolkit. It
// exercises both the drop case (duration below threshold and status ==
// successValue) and the fail-open keep case (unrelated status), and confirms
// the differential projection guard reports no divergence for this
// well-behaved sampler (it reads exactly what it projects).
func TestLoadSO_SeedPlugin(t *testing.T) {
	tmpDir, mkErr := os.MkdirTemp("", "sdktest-seed-*")
	require.NoError(t, mkErr)
	defer os.RemoveAll(tmpDir)

	soPath := buildSeedPlugin(t, tmpDir)

	sampler, loadErr := sdktest.LoadSO(soPath, "NewSampler", []byte(`{"thresholdMs":300,"successValue":"success"}`))
	if isToolchainMismatch(loadErr) {
		t.Skipf("LoadSO test skipped: host cannot load a freshly built plugin (toolchain mismatch): %v", loadErr)
	}
	require.NoError(t, loadErr)
	require.NotNil(t, sampler)
	defer sampler.Close() //nolint:errcheck // best-effort cleanup in a test.

	dropTrace, buildErr := sdktest.NewTrace("dropped").
		Tag("duration", int64(100)).
		Tag("status", "success").
		Build()
	require.NoError(t, buildErr)

	keepTrace, buildErr := sdktest.NewTrace("kept").
		Tag("duration", int64(100)).
		Tag("status", "failure").
		Build()
	require.NoError(t, buildErr)

	batch := sdktest.Batch(dropTrace, keepTrace)
	verdict, report := sdktest.Run(sampler, batch)
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	require.Empty(t, report.ProjectionDivergedIDs,
		"latencystatussampler only reads what it projects; the differential guard must report no divergence")
	require.Equal(t, []bool{false, true}, verdict.Keep,
		"the below-threshold+success trace must be dropped, the other kept")
}
