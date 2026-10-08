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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package nidx03rollback

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	// envRollbackDir overrides where the series-index rollback corpus (the
	// current repo's TestGenerateNIDX03RollbackCorpus output) lives. Must
	// match what that test itself was run with.
	envRollbackDir        = "NIDX03_ROLLBACK_DIR"
	defaultRollbackDir    = "/mnt/d/tmp-gao-build/nidx03-rollback"
	legacySeriesTemplate  = "legacy_series_index_read.go.tmpl"
	legacySeriesTestFile  = "nidx03_series_index_read_test.go"
	legacySeriesTestsPath = "banyand/internal/storage"
)

// TestNIDX03SeriesIndexRollback is the series-index-level half of the §12
// item 2 rollback proof: Measure normal mode, Measure index mode, and the
// crash-cut variant, each opened by the previous release's own
// seriesIndex/newSeriesIndex/Search/SearchWithoutSeries code (not a raw
// third-party-library proxy).
//
// It orchestrates three steps that are each independently reproducible by
// hand (the commands this function runs are the exact commands a human
// would type):
//  1. Generate the corpus with the CURRENT repo's code:
//     `NIDX03_ROLLBACK_WRITE=1 go test ./banyand/internal/storage/... -run TestGenerateNIDX03RollbackCorpus`
//  2. Stage legacy_series_index_read.go.tmpl into a `git archive
//     v0.11.1` export's banyand/internal/storage/ (PreviousReleaseExport).
//  3. Run that export's own `go test ./banyand/internal/storage/... -run
//     TestNIDX03PreviousReleaseOpens` -- which both opens the corpus with
//     the previous release's code AND asserts (via require.JSONEq inside
//     legacy_series_index_read.go.tmpl) that the results match the current
//     repo's own capture. A non-zero exit here is this test's failure, not
//     a human diff.
func TestNIDX03SeriesIndexRollback(t *testing.T) {
	if os.Getenv(envEnable) == "" {
		t.Skip("one-off rollback proof; set NIDX03_ROLLBACK=1 to run it")
	}
	repoRoot := repositoryRoot(t)
	rollbackDir := os.Getenv(envRollbackDir)
	if rollbackDir == "" {
		rollbackDir = defaultRollbackDir
	}
	require.NoError(t, os.RemoveAll(rollbackDir))

	// Step 1: generate the corpus with the current repo's code.
	generate := exec.Command("go", "test", "./banyand/internal/storage/...",
		"-run", "TestGenerateNIDX03RollbackCorpus", "-v", "-count=1", "-timeout=5m")
	generate.Dir = repoRoot
	generate.Env = append(os.Environ(), "NIDX03_ROLLBACK_WRITE=1", envRollbackDir+"="+rollbackDir)
	output, genErr := generate.CombinedOutput()
	require.NoErrorf(t, genErr, "corpus generation failed:\n%s", output)

	// Step 2: stage the committed read-side template into a previous-release
	// export.
	exportDir := PreviousReleaseExport(t)
	templatePath := filepath.Join(repoRoot, "test", "rollback", "nidx03", legacySeriesTemplate)
	templateBytes, readErr := os.ReadFile(templatePath) //nolint:gosec // fixed, repo-relative path.
	require.NoError(t, readErr)
	destPath := filepath.Join(exportDir, legacySeriesTestsPath, legacySeriesTestFile)
	require.NoError(t, os.WriteFile(destPath, templateBytes, 0o600))

	// Step 3: run the previous release's own test suite against the corpus.
	read := exec.Command("go", "test", "./"+legacySeriesTestsPath+"/...",
		"-run", "TestNIDX03PreviousReleaseOpens", "-v", "-count=1", "-timeout=5m")
	read.Dir = exportDir
	read.Env = append(os.Environ(), envRollbackDir+"="+rollbackDir)
	output, readErr = read.CombinedOutput()
	require.NoErrorf(t, readErr, "previous release (%s) did not reproduce the new code's query results:\n%s",
		previousReleaseTagFromEnv(), output)
	t.Logf("previous release (%s) series-index rollback output:\n%s", previousReleaseTagFromEnv(), output)
}
