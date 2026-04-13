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

func TestCrashOutputConfigInstallGlobalCrashOutput(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	cfg := CrashOutputConfig{
		Enabled: true,
		Dir:     tempDir,
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

func TestCrashOutputConfigInstallGlobalCrashOutputRequiresDir(t *testing.T) {
	t.Helper()

	cfg := CrashOutputConfig{
		Enabled: true,
	}

	if err := cfg.InstallGlobalCrashOutput(); err == nil {
		t.Fatal("expected empty dir to fail")
	}
}
