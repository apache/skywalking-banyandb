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

package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

func parsedImport(t *testing.T, args ...string) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	return parseDataCmd(t, newDataImportCmd, append([]string{"--nodes", "127.0.0.1:1"}, args...)...)
}

func preRunImport(t *testing.T, args ...string) (string, error) {
	t.Helper()
	c, errOut := parsedImport(t, args...)
	err := c.PreRunE(c, nil)
	return errOut.String(), err
}

func TestDataImport_FlagValidation(t *testing.T) {
	in := t.TempDir()
	for _, c := range []struct {
		want string
		args []string
	}{
		{"no artifact: give --input", nil},
		{"--write-batch-size: count must be an integer >= 1", []string{"--input", in, "--write-batch-size", "0"}},
		{"--write-batch-size", []string{"--input", in, "--write-batch-size", "10001"}},
		{"--write-batch-bytes", []string{"--input", in, "--write-batch-bytes", "1KiB"}},
		{"--schema-only needs the schema", []string{"--input", in, "--schema-only", "--include-schema=false"}},
		{"--verify-timeout has no effect", []string{"--input", in, "--verify=false", "--verify-timeout", "1m"}},
		{"--verify-timeout: 0s must be positive", []string{"--input", in, "--verify-timeout", "0s"}},
		{"--write-inflight-batches: 9 is outside [1, 8]", []string{"--input", in, "--write-inflight-batches", "9"}},
		{"--force confirms an overwrite", []string{"--input", in, "--force"}},
		{"unsupported schema conflict policy", []string{"--input", in, "--on-schema-conflict", "keep"}},
		{"output-format", []string{"--input", in, "-o", "csv"}},
	} {
		_, err := preRunImport(t, c.args...)
		requireUsage(t, err, c.want)
	}
	_, err := preRunImport(t, "--input", in, "--force", "--on-schema-conflict", "overwrite", "--write-batch-bytes", "64KiB", "--parallelism", "3",
		"--verify-timeout", "5s", "--write-inflight-batches", "1")
	require.NoError(t, err)
}

func TestDataImport_DryRunNamesTheIgnoredFlags(t *testing.T) {
	in := t.TempDir()
	out, err := preRunImport(t, "--input", in, "--dry-run", "--parallelism", "2", "--verify=false", "--verify-timeout", "1m", "--force-ttl")
	require.NoError(t, err, "a dry run ignores the push settings instead of refusing them")
	assert.Equal(t, "NOTE --dry-run writes nothing and ignores --verify, --verify-timeout, --parallelism\n", out)

	_, err = preRunImport(t, "--input", in, "--dry-run", "--write-batch-bytes", "1KiB")
	requireUsage(t, err, "--write-batch-bytes: 1024 bytes is outside [64KiB, 64MiB]")

	out, err = preRunImport(t, "--input", in, "--dry-run", "--write-batch-bytes", "64KiB")
	require.NoError(t, err)
	assert.Empty(t, out, "the dry run plan uses --write-batch-bytes, so the notice must not call it ignored")

	out, err = preRunImport(t, "--input", in, "--dry-run", "--write-batch-size", "20000")
	require.NoError(t, err)
	assert.Equal(t, "NOTE --dry-run writes nothing and ignores --write-batch-size\n", out)

	out, err = preRunImport(t, "--input", in, "--dry-run")
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestDataImport_PlanFileSettings(t *testing.T) {
	dir := t.TempDir()
	planFile := filepath.Join(dir, "plan.yaml")
	body := "import:\n  input: " + dir + "\n  writeBatchSize: 0\n"
	require.NoError(t, os.WriteFile(planFile, []byte(body), 0o600))
	_, err := preRunImport(t, "--plan", planFile)
	requireUsage(t, err, "plan.yaml import.writeBatchSize")

	body = "import:\n  input: " + dir + "\n  schemaConflictPolicy:\n    rules:\n      - match: stream/g\n        policy: overwrite\n"
	require.NoError(t, os.WriteFile(planFile, []byte(body), 0o600))
	_, err = preRunImport(t, "--plan", planFile, "--force")
	require.NoError(t, err, "a plan.yaml overwrite rule makes --force meaningful")
}

func TestDataImport_RootScopeFlagsNameTheCommand(t *testing.T) {
	ResetFlags()
	t.Cleanup(ResetFlags)
	explicitRootFlags["group"] = true
	requireUsage(t, rejectRootScopeFlags("import"), "the groups come from the artifact (--input)")
	requireUsage(t, rejectRootScopeFlags("export"), "the scope comes from --selector")
	explicitRootFlags = map[string]bool{"addr": true}
	requireUsage(t, rejectRootScopeFlags("import"), "data import takes liaison gRPC addresses via --nodes")
}

func TestDataImport_UnreachableLiaisonIsAPreflightRefusal(t *testing.T) {
	c, errOut := parsedImport(t, "--input", t.TempDir())
	c.SetContext(context.Background())
	require.NoError(t, c.PreRunE(c, nil))
	err := c.RunE(c, nil)
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "%v", err)
	assert.Equal(t, exitcode.ExitPreflight, exitErr.Code)
	assert.Contains(t, errOut.String(), "liaisons: 127.0.0.1:1 (from --nodes)")
}
