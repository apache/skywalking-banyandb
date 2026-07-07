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
	"testing"
)

// sharedPluginDir is the package-level directory that holds the single .so
// used by all tests that need a real plugin. It is created by TestMain before
// any test runs, so the Go plugin runtime only ever opens the .so once
// (avoids the "plugin already loaded" deduplication error that fires when two
// tests build the same content into different paths).
var sharedPluginDir string

// TestMain is the package-level test harness. It creates sharedPluginDir so
// that pipeline_loader_test.go (TestLoadSamplerPlugin) and
// pipeline_watch_test.go (TestReconcilePipeline_*) share a single .so path.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "trace-pkg-test-plugin-*")
	if err == nil {
		sharedPluginDir = dir
	}
	// Run all tests; clean up the shared dir afterward.
	code := m.Run()
	if sharedPluginDir != "" {
		os.RemoveAll(sharedPluginDir)
	}
	os.Exit(code)
}
