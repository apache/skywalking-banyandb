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

package lintrawgo

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestAnalyzer_BareAndDirective covers three behaviors at once:
//   - a raw `go func()` is reported,
//   - a raw `go ident()` is reported,
//   - a //panicdiag:allow-rawgo directive with a reason suppresses, but a
//     directive without a reason does not.
//
// Expected sites are marked with `// want` annotations in the fixture.
func TestAnalyzer_BareAndDirective(t *testing.T) {
	a := newAnalyzer()
	dir := analysistest.TestData()
	analysistest.Run(t, dir, a, "badcode")
}

// TestAnalyzer_BaselineSuppresses verifies that a baseline entry covering
// the exact (relative-path, line) of a violation removes the diagnostic.
// The fixture has no `// want` annotations; analysistest treats absence
// of expectations as "no diagnostics expected", so an unexpected
// diagnostic would fail the test.
func TestAnalyzer_BaselineSuppresses(t *testing.T) {
	dir := analysistest.TestData()
	fixture := filepath.Join(dir, "src", "baselined", "baselined.go")
	abs, err := filepath.Abs(fixture)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	rel := moduleRelative(abs)

	// The bare go statement in baselined.go sits on line 24 (after the
	// license header and package doc). Grandfather that exact line.
	baselineFile := filepath.Join(t.TempDir(), "baseline.txt")
	body := "# test baseline\n" + rel + ":24\n"
	if err := os.WriteFile(baselineFile, []byte(body), 0o600); err != nil {
		t.Fatalf("write baseline: %v", err)
	}

	a := newAnalyzer()
	if err := a.Flags.Set("baseline", baselineFile); err != nil {
		t.Fatalf("set baseline flag: %v", err)
	}
	analysistest.Run(t, dir, a, "baselined")
}

// TestLoadBaseline_Format validates the baseline parser accepts the
// documented format (path:line, comments, blank lines) and rejects the
// obvious ill-formed inputs that would otherwise silently grandfather
// nothing.
func TestLoadBaseline_Format(t *testing.T) {
	good := "# header comment\n\npkg/foo/bar.go:10\npkg/foo/bar.go:42 # inline comment\n"
	path := filepath.Join(t.TempDir(), "baseline.txt")
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	m, err := loadBaseline(path)
	if err != nil {
		t.Fatalf("loadBaseline: %v", err)
	}
	if !m["pkg/foo/bar.go"][10] || !m["pkg/foo/bar.go"][42] {
		t.Fatalf("baseline missing entries: %#v", m)
	}

	bad := "pkg/foo/bar.go\n" // missing :line
	badPath := filepath.Join(t.TempDir(), "baseline.txt")
	if err := os.WriteFile(badPath, []byte(bad), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := loadBaseline(badPath); err == nil {
		t.Fatal("expected error for malformed baseline, got nil")
	}
}
