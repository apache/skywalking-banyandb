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
	"os/exec"
	"path/filepath"
	"plugin"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	pipelinev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/pipeline/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// isPluginToolchainMismatch reports whether err is the Go runtime's rejection of
// a .so whose build context differs from the host's (race mode or compiler
// version skew). plugin.Open surfaces this as "different version of package ...".
func isPluginToolchainMismatch(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "different version of package") ||
		strings.Contains(msg, "plugin was built with a different version")
}

// pluginPkgPath is the path to the latencystatussampler plugin source relative
// to this file's directory (banyand/trace).
const pluginPkgPath = "../../test/plugins/_latencystatussampler"

// buildTestPlugin compiles the latencystatussampler plugin into dir and returns
// the absolute path to the resulting .so. If the build is not possible (CGO
// unavailable, no C toolchain, or go not found), the test is skipped with an
// actionable message.
func buildTestPlugin(t *testing.T, dir string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Go plugins are not supported on Windows")
	}

	goExe, lookErr := exec.LookPath("go")
	if lookErr != nil {
		t.Skip("loadSamplerPlugin test skipped: 'go' binary not found in PATH")
	}

	soPath := filepath.Join(dir, "lss.so")

	// Resolve the plugin source directory from this file's location so the path
	// is correct regardless of the working directory when tests are run.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("loadSamplerPlugin test skipped: cannot determine source file path via runtime.Caller")
	}
	absPluginDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), pluginPkgPath))

	// Do NOT pass -trimpath: go test is not built with -trimpath, and
	// plugin.Open requires the host and plugin to share the same package path
	// encoding. Omitting -trimpath keeps them consistent.
	// Production builds (Make target) use -trimpath on both the server and the
	// .so; that is exercised by the integration suite, not this unit test.
	//
	// Match the host's race mode: CI runs `ginkgo --race`, and a race-instrumented
	// host can only load a .so also built with -race (the Go runtime otherwise
	// rejects the mismatch with "different version of package internal/runtime/sys").
	args := []string{"build", "-buildmode=plugin"}
	if raceDetectorEnabled {
		args = append(args, "-race")
	}
	args = append(args, "-o", soPath, absPluginDir)
	cmd := exec.Command(goExe, args...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, buildErr := cmd.CombinedOutput()
	if buildErr != nil {
		t.Skipf("loadSamplerPlugin test skipped: cannot build .so (CGO may be unavailable): %v\n%s", buildErr, out)
	}
	return soPath
}

// resetPluginCache wipes the process-global plugin cache so each sub-test
// starts from a clean state at the loadSamplerPlugin layer. Note: this does
// NOT unload any .so already opened by the Go plugin runtime — that is
// permanent for the process lifetime, which is intentional for the wrong-symbol
// guard (it needs the .so already open so Lookup can be reached).
func resetPluginCache() {
	pluginCache.mu.Lock()
	pluginCache.m = make(map[pluginCacheKey]sdk.Sampler)
	pluginCache.mu.Unlock()
}

// TestLoadSamplerPlugin exercises loadSamplerPlugin's happy path and every
// guard in a single test function so the .so is built once and the same path
// is used throughout. This matters because the Go plugin runtime deduplicates
// opens by content hash: a second plugin.Open of an identical-content .so at a
// different path returns "plugin already loaded" instead of succeeding.
// Using a shared path lets plugin.Open return the already-loaded *plugin.Plugin
// on guard sub-tests that need to reach Lookup (e.g. wrong constructor symbol).
func TestLoadSamplerPlugin(t *testing.T) {
	tmpDir, mkdirErr := os.MkdirTemp("", "lssplugin-test-*")
	require.NoError(t, mkdirErr)
	defer os.RemoveAll(tmpDir)

	soPath := buildTestPlugin(t, tmpDir)
	soName := filepath.Base(soPath) // relative name passed to loadSamplerPlugin

	// Defensive probe: even with a matching race mode, a compiler-version skew
	// between the toolchain that built the test binary and the `go` in PATH makes
	// plugin.Open reject the freshly built .so. Treat that as an environment skip,
	// not a failure. On success this also pre-opens the .so, which the
	// wrong-constructor-symbol guard below relies on to reach Lookup.
	if _, probeErr := plugin.Open(soPath); isPluginToolchainMismatch(probeErr) {
		t.Skipf("loadSamplerPlugin test skipped: host cannot load a freshly built plugin (toolchain mismatch): %v", probeErr)
	}

	t.Run("happy path", func(t *testing.T) {
		resetPluginCache()

		cfgStruct, cfgErr := structpb.NewStruct(map[string]interface{}{
			"thresholdMs":  float64(300),
			"successValue": "ok",
		})
		require.NoError(t, cfgErr)

		sp := &pipelinev1.SamplerPlugin{
			Path:       soName,
			Symbol:     "NewSampler",
			AbiVersion: uint32(sdk.ABIVersion),
			Config:     cfgStruct,
		}

		sampler, loadErr := loadSamplerPlugin(sp, tmpDir)
		require.NoError(t, loadErr)
		require.NotNil(t, sampler)

		assert.Equal(t, sdk.KindSampler, sampler.Kind())

		proj := sampler.Project()
		assert.Contains(t, proj.Tags, "duration", "plugin must project the duration tag")
		assert.Contains(t, proj.Tags, "status", "plugin must project the status tag")
	})

	// Guard sub-tests: each expects an error from loadSamplerPlugin.
	// The missing-path and trusted-dir-escape guards never reach plugin.Open.
	// The ABI-mismatch guard fires before plugin.Open.
	// The wrong-symbol guard requires plugin.Open to succeed (the .so is already
	// open from the happy-path sub-test above) so that Lookup can be reached.
	guardTests := []struct {
		name        string
		sp          *pipelinev1.SamplerPlugin
		trustedDir  string
		errContains string
	}{
		{
			name: "missing path",
			sp: &pipelinev1.SamplerPlugin{
				Path:       "",
				Symbol:     "NewSampler",
				AbiVersion: uint32(sdk.ABIVersion),
			},
			trustedDir:  tmpDir,
			errContains: "plugin path is empty",
		},
		{
			name: "ABI mismatch",
			sp: &pipelinev1.SamplerPlugin{
				Path:       soName,
				Symbol:     "NewSampler",
				AbiVersion: 999,
			},
			trustedDir:  tmpDir,
			errContains: "ABI version",
		},
		{
			name: "trusted-dir escape",
			sp: &pipelinev1.SamplerPlugin{
				Path:       "../escape.so",
				Symbol:     "NewSampler",
				AbiVersion: uint32(sdk.ABIVersion),
			},
			trustedDir:  tmpDir,
			errContains: "escapes trusted directory",
		},
		{
			// The .so is already open from the happy-path sub-test above.
			// plugin.Open returns the cached *plugin.Plugin, Lookup then fails
			// because "DoesNotExist" is not exported by the plugin.
			name: "wrong constructor symbol",
			sp: &pipelinev1.SamplerPlugin{
				Path:       soName,
				Symbol:     "DoesNotExist",
				AbiVersion: uint32(sdk.ABIVersion),
			},
			trustedDir:  tmpDir,
			errContains: "missing symbol",
		},
	}

	for _, tc := range guardTests {
		t.Run(tc.name, func(t *testing.T) {
			resetPluginCache()

			sampler, loadErr := loadSamplerPlugin(tc.sp, tc.trustedDir)
			require.Error(t, loadErr, "expected an error for guard %q", tc.name)
			assert.Nil(t, sampler)
			assert.Contains(t, loadErr.Error(), tc.errContains,
				"error message must contain %q", tc.errContains)
		})
	}
}

// TestLoadSamplerPlugin_DotDotPrefixChildNotEscape verifies the trusted-dir guard
// does not falsely reject a legitimate child whose first path segment merely
// starts with ".." (e.g. "..foo/..."). Such a path is inside the trusted dir, so
// the load must pass the escape guard and instead fail to open the (nonexistent)
// file — never be rejected as an escape. No .so toolchain is needed; the path is
// intentionally absent.
func TestLoadSamplerPlugin_DotDotPrefixChildNotEscape(t *testing.T) {
	tmpDir, mkdirErr := os.MkdirTemp("", "lssplugin-escape-*")
	require.NoError(t, mkdirErr)
	defer os.RemoveAll(tmpDir)
	resetPluginCache()

	sp := &pipelinev1.SamplerPlugin{
		Path:       "..foo/nonexistent.so",
		Symbol:     "NewSampler",
		AbiVersion: uint32(sdk.ABIVersion),
	}
	_, loadErr := loadSamplerPlugin(sp, tmpDir)
	require.Error(t, loadErr, "opening a nonexistent file must still error")
	assert.NotContains(t, loadErr.Error(), "escapes trusted directory",
		"a '..'-prefixed child path inside the trusted dir must not be rejected as an escape")
}
