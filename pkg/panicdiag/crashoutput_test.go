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
	"path/filepath"
	"runtime/debug"
	"testing"
)

func TestNewCrashOutputConfigDefaultsToEnabled(t *testing.T) {
	t.Helper()

	cfg := NewCrashOutputConfig()
	if !cfg.Enabled {
		t.Fatal("NewCrashOutputConfig must default to Enabled=true — crash output is the final safety net")
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

	called := false
	var capturedName string
	originalSetCrashOutput := setCrashOutput
	setCrashOutput = func(file *os.File, _ debug.CrashOptions) error {
		called = true
		if file == nil {
			t.Fatal("expected crash file")
		}
		capturedName = file.Name()
		return nil
	}
	defer func() {
		setCrashOutput = originalSetCrashOutput
	}()

	if err := cfg.InstallGlobalCrashOutput(); err != nil {
		t.Fatalf("install global crash output: %v", err)
	}

	if !called {
		t.Fatal("expected SetCrashOutput to be called")
	}

	expectedPath := filepath.Join(tempDir, runtimeCrashFileName())
	if capturedName != expectedPath {
		t.Fatalf("crash output path mismatch: got %s want %s", capturedName, expectedPath)
	}

	info, err := os.Stat(expectedPath)
	if err != nil {
		t.Fatalf("stat crash output file: %v", err)
	}
	if info.IsDir() {
		t.Fatal("expected crash output file, got directory")
	}
}

func TestCrashOutputConfigInstallGlobalCrashOutputDisabled(t *testing.T) {
	t.Helper()

	cfg := CrashOutputConfig{
		Enabled: false,
		Dir:     t.TempDir(),
	}

	originalSetCrashOutput := setCrashOutput
	setCrashOutput = func(_ *os.File, _ debug.CrashOptions) error {
		t.Fatal("SetCrashOutput should not be called when disabled")
		return nil
	}
	defer func() {
		setCrashOutput = originalSetCrashOutput
	}()

	if err := cfg.InstallGlobalCrashOutput(); err != nil {
		t.Fatalf("install global crash output: %v", err)
	}
}

func TestCrashOutputConfigInstallSetsMaxArtifacts(t *testing.T) {
	t.Helper()

	originalSetCrashOutput := setCrashOutput
	setCrashOutput = func(_ *os.File, _ debug.CrashOptions) error { return nil }
	defer func() { setCrashOutput = originalSetCrashOutput }()

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

	originalSetCrashOutput := setCrashOutput
	setCrashOutput = func(_ *os.File, _ debug.CrashOptions) error { return nil }
	defer func() { setCrashOutput = originalSetCrashOutput }()

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

func TestCleanupGlobalCrashOutputRemovesEmptyFile(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	crashPath := filepath.Join(tempDir, runtimeCrashFileName())
	crashFile, openErr := os.OpenFile(crashPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if openErr != nil {
		t.Fatalf("open crash output file: %v", openErr)
	}

	globalCrashFile = crashFile
	globalCrashPath = crashPath

	if cleanupErr := CleanupGlobalCrashOutput(); cleanupErr != nil {
		t.Fatalf("cleanup global crash output: %v", cleanupErr)
	}

	if _, statErr := os.Stat(crashPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected crash output file to be removed, got stat err: %v", statErr)
	}
}

func TestCleanupGlobalCrashOutputKeepsNonEmptyFile(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	crashPath := filepath.Join(tempDir, runtimeCrashFileName())
	crashFile, openErr := os.OpenFile(crashPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if openErr != nil {
		t.Fatalf("open crash output file: %v", openErr)
	}
	if _, writeErr := crashFile.WriteString("fatal runtime output\n"); writeErr != nil {
		t.Fatalf("write crash output file: %v", writeErr)
	}

	globalCrashFile = crashFile
	globalCrashPath = crashPath

	if cleanupErr := CleanupGlobalCrashOutput(); cleanupErr != nil {
		t.Fatalf("cleanup global crash output: %v", cleanupErr)
	}

	info, statErr := os.Stat(crashPath)
	if statErr != nil {
		t.Fatalf("stat crash output file: %v", statErr)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty crash output file to be preserved")
	}
}
