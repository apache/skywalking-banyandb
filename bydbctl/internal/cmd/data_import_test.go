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

	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

func parsedImport(t *testing.T, args ...string) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	return parseDataCmd(t, newDataImportCmd, append([]string{"--nodes", "127.0.0.1:1"}, args...)...)
}

func preRunImport(t *testing.T, args ...string) (string, error) {
	t.Helper()
	c, out := parsedImport(t, args...)
	err := c.PreRunE(c, nil)
	return out.String(), err
}

// The notice about the flags a dry run ignores is information: with a table it is on stdout, with
// json or yaml it goes to stderr and stdout stays free for the rendering.
func TestDataImport_NoticeFollowsTheOutputFormat(t *testing.T) {
	in := t.TempDir()
	const notice = "NOTE --dry-run writes nothing and ignores --parallelism\n"
	c, out := parsedImport(t, "--input", in, "--dry-run", "--parallelism", "2")
	require.NoError(t, c.PreRunE(c, nil))
	assert.Equal(t, notice, out.String())
	assert.Empty(t, stderrOf(c))

	for _, format := range []string{"json", "yaml"} {
		c, out = parsedImport(t, "--input", in, "--dry-run", "--parallelism", "2", "-o", format)
		require.NoError(t, c.PreRunE(c, nil))
		assert.Empty(t, out.String(), format)
		assert.Equal(t, notice, stderrOf(c), format)
	}
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
		{"--verify-timeout has no effect", []string{"--input", in, "--schema-only", "--verify-timeout", "1m"}},
		{"--verify-timeout: 0s must be positive", []string{"--input", in, "--verify-timeout", "0s"}},
		{"--write-inflight-batches: 9 is outside [1, 8]", []string{"--input", in, "--write-inflight-batches", "9"}},
		{"--force confirms an overwrite", []string{"--input", in, "--force"}},
		{"unsupported schema conflict policy", []string{"--input", in, "--on-schema-conflict", "keep"}},
		{"output-format", []string{"--input", in, "-o", "csv"}},
		{"--checkpoint-every-batches: 10001 is outside [1, 10000]", []string{"--input", in, "--checkpoint-every-batches", "10001"}},
		{"the name of a side file of a progress file", []string{"--input", in, "--checkpoint", filepath.Join(in, "p-plan.json")}},
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

	out, err = preRunImport(t, "--input", in, "--dry-run", "--resume-duplication-policy", "block")
	require.NoError(t, err)
	assert.Empty(t, out, "the dry run resume summary uses --resume-duplication-policy, so the notice must not call it ignored")

	out, err = preRunImport(t, "--input", in, "--dry-run", "--write-batch-size", "20000")
	require.NoError(t, err)
	assert.Equal(t, "NOTE --dry-run writes nothing and ignores --write-batch-size\n", out)

	out, err = preRunImport(t, "--input", in, "--dry-run", "--part-rate-limit", "1MiB/s", "--part-chunk-size", "1MiB")
	require.NoError(t, err)
	assert.Equal(t, "NOTE --dry-run writes nothing and ignores --part-rate-limit, --part-chunk-size\n", out)

	out, err = preRunImport(t, "--input", in, "--dry-run", "--restart")
	require.NoError(t, err)
	assert.Empty(t, out, "a dry run with --restart previews a fresh import, so the notice must not call it ignored")

	out, err = preRunImport(t, "--input", in, "--dry-run", "--resume")
	require.NoError(t, err)
	assert.Equal(t, "NOTE --dry-run writes nothing and ignores --resume\n", out)

	out, err = preRunImport(t, "--input", in, "--dry-run")
	require.NoError(t, err)
	assert.Empty(t, out)
}

// Only an explicit --verify asks to verify a finished import again: plan.yaml and
// BYDBCTL_IMPORT_VERIFY set a default, as does the default itself.
func TestDataImport_VerifyExplicit(t *testing.T) {
	plan := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(plan, []byte("import:\n  verify: true\n"), 0o600))
	for _, c := range []struct {
		env  string
		args []string
		want bool
	}{{args: []string{"--verify"}, want: true}, {env: "true"}, {args: []string{"--plan", plan}}, {}} {
		t.Setenv("BYDBCTL_IMPORT_VERIFY", c.env) // empty counts as unset
		cmd, _ := parsedImport(t, c.args...)
		opts := &config.Import{}
		sources, err := resolveSettings(cmd, &config.Connection{}, opts)
		require.NoError(t, err)
		assert.Equal(t, c.want, sourcedEnv(sources, opts).VerifyExplicit, "%v BYDBCTL_IMPORT_VERIFY=%q", c.args, c.env)
	}
}

// The resume summary names where the duplication policy was set: the flag, the variable or the plan.
func TestDataImport_DuplicationSourceNamesWhereThePolicyWasSet(t *testing.T) {
	plan := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(plan, []byte("import:\n  resumeDuplicationPolicy: block\n"), 0o600))
	for _, c := range []struct {
		env  string
		want string
		args []string
	}{
		{args: []string{"--resume-duplication-policy", "skip"}, want: "--resume-duplication-policy"},
		{env: "push", want: "BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY"},
		{args: []string{"--plan", plan}, want: "plan.yaml import.resumeDuplicationPolicy"},
	} {
		t.Setenv("BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY", c.env)
		cmd, _ := parsedImport(t, c.args...)
		opts := &config.Import{}
		sources, err := resolveSettings(cmd, &config.Connection{}, opts)
		require.NoError(t, err)
		assert.Equal(t, c.want, sourcedEnv(sources, opts).DuplicationSource, "%v BYDBCTL_IMPORT_RESUME_DUPLICATION_POLICY=%q", c.args, c.env)
		_, source := opts.ResolveDuplicationPolicy(sourcedEnv(sources, opts).DuplicationSource)
		assert.Equal(t, c.want, source)
	}
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
	c, out := parsedImport(t, "--input", t.TempDir())
	c.SetContext(context.Background())
	require.NoError(t, c.PreRunE(c, nil))
	err := c.RunE(c, nil)
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "%v", err)
	assert.Equal(t, exitcode.ExitPreflight, exitErr.Code)
	assert.Contains(t, out.String(), "liaisons: 127.0.0.1:1 (from --nodes)")
}
