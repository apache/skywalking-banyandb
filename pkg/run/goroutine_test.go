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

package run

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

func TestGo_RecoveryCapturesBreadcrumbsAddedInFn(t *testing.T) {
	t.Helper()

	if os.Getenv("BANYANDB_RUN_GO_REPANIC_HELPER") == "1" {
		artifactRoot := os.Getenv("BANYANDB_RUN_GO_ARTIFACT_ROOT")
		require.NotEmpty(t, artifactRoot)
		panicdiag.SetDefaultArtifactRoot(artifactRoot)

		Go(context.Background(), "test", logger.GetLogger("test"), func(ctx context.Context) {
			panicdiag.WithBreadcrumb(ctx, "stage-A", "component", nil)
			panicdiag.WithBreadcrumb(ctx, "stage-B", "component", nil)
			panic("boom")
		})

		time.Sleep(5 * time.Second)
		t.Fatal("expected run.Go panic to terminate helper process")
	}

	artifactRoot := t.TempDir()
	testCmd := exec.Command(os.Args[0], "-test.run=TestGo_RecoveryCapturesBreadcrumbsAddedInFn")
	testCmd.Env = append(os.Environ(),
		"BANYANDB_RUN_GO_REPANIC_HELPER=1",
		"BANYANDB_RUN_GO_ARTIFACT_ROOT="+artifactRoot,
	)
	runErr := testCmd.Run()
	require.Error(t, runErr)
	var exitErr *exec.ExitError
	require.ErrorAs(t, runErr, &exitErr)

	collections, listErr := panicdiag.ListCollections(artifactRoot)
	require.NoError(t, listErr)
	require.Len(t, collections, 1)
	require.NotNil(t, collections[0].Record)
	require.Equal(t, "boom", collections[0].Record.PanicValue)
	require.Len(t, collections[0].Record.Breadcrumbs, 2)
	require.Equal(t, "stage-A", collections[0].Record.Breadcrumbs[0].Stage)
	require.Equal(t, "stage-B", collections[0].Record.Breadcrumbs[1].Stage)
}

func TestGo_ReportsBeforeRepanic(t *testing.T) {
	t.Helper()

	if os.Getenv("BANYANDB_RUN_GO_REPORTER_HELPER") == "1" {
		reportPath := os.Getenv("BANYANDB_RUN_GO_REPORT_PATH")
		require.NotEmpty(t, reportPath)

		Go(context.Background(), "test", logger.GetLogger("test"), func(_ context.Context) {
			panic("reported boom")
		}, func(_ context.Context, result panicdiag.RecoveryResult) {
			if result.Record == nil {
				return
			}
			_ = os.WriteFile(reportPath, []byte(result.Record.PanicValue), 0o600)
		})

		time.Sleep(5 * time.Second)
		t.Fatal("expected run.Go panic to terminate helper process")
	}

	reportPath := t.TempDir() + "/panic-report"
	testCmd := exec.Command(os.Args[0], "-test.run=TestGo_ReportsBeforeRepanic")
	testCmd.Env = append(os.Environ(),
		"BANYANDB_RUN_GO_REPORTER_HELPER=1",
		"BANYANDB_RUN_GO_REPORT_PATH="+reportPath,
	)
	runErr := testCmd.Run()
	require.Error(t, runErr)
	var exitErr *exec.ExitError
	require.ErrorAs(t, runErr, &exitErr)

	reportData, readErr := os.ReadFile(reportPath)
	require.NoError(t, readErr)
	require.Equal(t, "reported boom", string(reportData))
}
