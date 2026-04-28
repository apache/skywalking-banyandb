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

package panicdiag

import (
	"os"
	"testing"
)

func TestNewCrashOutputConfigDefaultsToEnabled(t *testing.T) {
	t.Helper()

	cfg := NewCrashOutputConfig()
	if !cfg.Enabled {
		t.Fatal("NewCrashOutputConfig must default to Enabled=true")
	}
	if cfg.Dir == "" {
		t.Fatal("NewCrashOutputConfig must set a default Dir")
	}
	if cfg.MaxArtifacts <= 0 {
		t.Fatalf("NewCrashOutputConfig must set a positive MaxArtifacts default, got %d", cfg.MaxArtifacts)
	}
	if cfg.GoMemLimitPct <= 0 {
		t.Fatalf("NewCrashOutputConfig must set a positive GoMemLimitPct default, got %d", cfg.GoMemLimitPct)
	}
}

func TestCrashOutputConfigInstallGlobalCrashOutput(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	cfg := CrashOutputConfig{
		Enabled:       true,
		Dir:           tempDir,
		MaxArtifacts:  5,
		GoMemLimitPct: 0, // disable to avoid cgroup calls in test
	}

	if err := cfg.InstallGlobalCrashOutput(); err != nil {
		t.Fatalf("install global crash output: %v", err)
	}
	if got := DefaultArtifactRoot(); got != tempDir {
		t.Fatalf("DefaultArtifactRoot: got %s want %s", got, tempDir)
	}
	entries, readErr := os.ReadDir(tempDir)
	if readErr != nil {
		t.Fatalf("read diagnostics dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no runtime crash files, got %d entries", len(entries))
	}
}

func TestCrashOutputConfigInstallGlobalCrashOutputDisabled(t *testing.T) {
	t.Helper()

	cfg := CrashOutputConfig{
		Enabled: false,
		Dir:     t.TempDir(),
	}

	if err := cfg.InstallGlobalCrashOutput(); err != nil {
		t.Fatalf("install global crash output: %v", err)
	}
}

func TestCrashOutputConfigInstallSetsMaxArtifacts(t *testing.T) {
	t.Helper()

	cfg := CrashOutputConfig{
		Enabled:       true,
		Dir:           t.TempDir(),
		MaxArtifacts:  7,
		GoMemLimitPct: 0,
	}
	if err := cfg.InstallGlobalCrashOutput(); err != nil {
		t.Fatalf("install global crash output: %v", err)
	}
	if got := DefaultMaxArtifacts(); got != 7 {
		t.Fatalf("DefaultMaxArtifacts: got %d want 7", got)
	}
}

func TestCrashOutputConfigInstallAppliesGoMemLimit(t *testing.T) {
	t.Helper()

	origCgroup := cgroupMemLimit
	origSet := setMemoryLimit
	defer func() {
		cgroupMemLimit = origCgroup
		setMemoryLimit = origSet
	}()

	const cgBytes = int64(2 * 1024 * 1024 * 1024) // 2 GiB
	var appliedLimit int64
	cgroupMemLimit = func() (int64, error) { return cgBytes, nil }
	setMemoryLimit = func(n int64) int64 {
		appliedLimit = n
		return 0
	}

	cfg := CrashOutputConfig{
		Enabled:       true,
		Dir:           t.TempDir(),
		MaxArtifacts:  0,
		GoMemLimitPct: 85,
	}
	if err := cfg.InstallGlobalCrashOutput(); err != nil {
		t.Fatalf("install global crash output: %v", err)
	}
	want := cgBytes * 85 / 100
	if appliedLimit != want {
		t.Fatalf("GOMEMLIMIT: got %d want %d", appliedLimit, want)
	}
}

func TestCrashOutputConfigInstallGlobalCrashOutputRequiresDir(t *testing.T) {
	t.Helper()

	cfg := CrashOutputConfig{
		Enabled: true,
	}

	if err := cfg.InstallGlobalCrashOutput(); err == nil {
		t.Fatal("expected empty dir to fail")
	}
}
