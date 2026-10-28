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

package config

import (
	"math"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boundImport(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	Bind(fs, &Connection{}, &Import{})
	require.NoError(t, fs.Parse(args))
	return fs
}

func resolveImport(t *testing.T, fs *pflag.FlagSet, planFile string, env map[string]string) (*Import, Sources, error) {
	t.Helper()
	if planFile != "" {
		require.NoError(t, fs.Set(PlanFlag, planFile))
	}
	imp := &Import{}
	sources, err := Resolve(fs, envOf(env), &Connection{}, imp)
	return imp, sources, err
}

func TestResolveImport_Defaults(t *testing.T) {
	imp, sources, err := resolveImport(t, boundImport(t), "", nil)
	require.NoError(t, err)
	assert.Empty(t, sources)
	assert.Equal(t, Import{
		OutputFormat: "table", OnSchemaConflict: ConflictPolicyError, WriteBatchBytes: 4 << 20,
		SendRetryTimeout: 15 * time.Minute, VerifyTimeout: 2 * time.Minute, Parallelism: math.MaxInt,
		WriteBatchSize: 1000, WriteInflightBatches: 2, IncludeSchema: true, Verify: true,
	}, *imp)
	require.NoError(t, imp.Validate(sources))
}

// importPlanYAML carries every import field of plan.yaml.
const importPlanYAML = `
connection:
  rateLimit: 100MiB/s
import:
  input: ./backup
  includeSchema: true
  schemaOnly: false
  schemaConflictPolicy:
    default: ignore
    rules:
      - match: group/sw_metric
        policy: overwrite
      - match: measure/sw_metric/service_cpm_minute
        policy: error
      - match: stream/sw_record
        policy: ignore
  parallelism: 4
  writeRateLimit: 3000000/min
  nodeRateLimit: 50MiB/s
  nodeRowRateLimit: 60000/min
  writeBatchSize: 500
  writeBatchBytes: 1MiB
  writeInflightBatches: 4
  sendRetryTimeout: 1h
  verify: false
  verifyTimeout: 5m
`

func TestResolveImport_Plan(t *testing.T) {
	imp, sources, err := resolveImport(t, boundImport(t), writePlan(t, importPlanYAML), nil)
	require.NoError(t, err)
	assert.Equal(t, Import{
		Input: "./backup", OutputFormat: "table", OnSchemaConflict: ConflictPolicyIgnore,
		ConflictRules: []ConflictRule{
			{Match: "group/sw_metric", Policy: ConflictPolicyOverwrite},
			{Match: "measure/sw_metric/service_cpm_minute", Policy: ConflictPolicyError},
			{Match: "stream/sw_record", Policy: ConflictPolicyIgnore},
		},
		WriteBatchBytes: 1 << 20, WriteRateLimit: 50000, LiaisonRateLimit: 100 << 20, NodeRateLimit: 50 << 20, NodeRowRateLimit: 1000,
		SendRetryTimeout: time.Hour, VerifyTimeout: 5 * time.Minute, Parallelism: 4, WriteBatchSize: 500,
		WriteInflightBatches: 4, IncludeSchema: true,
	}, *imp)
	assert.Equal(t, "plan.yaml import.schemaConflictPolicy.rules", sources["import.schemaConflictPolicy.rules"],
		"a plan-only setting is keyed by its plan path")
	assert.True(t, sources.FromPlan("import.schemaConflictPolicy.rules"))
	assert.True(t, sources.FromPlan("on-schema-conflict"))
	assert.True(t, imp.HasOverwrite())
}

func TestResolveImport_PlanOnlyField(t *testing.T) {
	fs := boundImport(t)
	fs.VisitAll(func(f *pflag.Flag) {
		assert.NotEqual(t, "-", f.Name, "a plan-only setting has no flag")
	})
	assert.Nil(t, fs.Lookup("-"))
	imp, sources, err := resolveImport(t, fs, "", map[string]string{"BYDBCTL_-": "x"})
	require.NoError(t, err)
	assert.Nil(t, imp.ConflictRules)
	assert.NotContains(t, sources, "import.schemaConflictPolicy.rules")
	assert.Equal(t, "plan.yaml import.schemaConflictPolicy.rules", sources.Label("import.schemaConflictPolicy.rules"),
		"a plan-only setting at its default is labeled by its plan path")
}

func TestResolveImport_Precedence(t *testing.T) {
	planFile := writePlan(t, "import:\n  writeBatchSize: 200\n  input: /plan\n  verifyTimeout: 1m\n")
	env := map[string]string{
		"BYDBCTL_IMPORT_WRITE_BATCH_SIZE": "300", "BYDBCTL_IMPORT_INPUT": "/env", "BYDBCTL_IMPORT_VERIFY_TIMEOUT": "3m",
		"BYDBCTL_IMPORT_PARALLELISM": "7", "BYDBCTL_IMPORT_ON_SCHEMA_CONFLICT": "ignore",
	}
	imp, sources, err := resolveImport(t, boundImport(t, "--write-batch-size", "100"), planFile, env)
	require.NoError(t, err)
	assert.Equal(t, 100, imp.WriteBatchSize, "an explicit flag beats plan.yaml")
	assert.Equal(t, "/plan", imp.Input, "plan.yaml beats the environment")
	assert.Equal(t, time.Minute, imp.VerifyTimeout)
	assert.Equal(t, 7, imp.Parallelism, "the environment beats the default")
	assert.Equal(t, ConflictPolicyIgnore, imp.OnSchemaConflict)
	assert.True(t, sources.FromFlag("write-batch-size"))
	assert.True(t, sources.FromPlan("input"))
	assert.Equal(t, "BYDBCTL_IMPORT_PARALLELISM", sources.Label("parallelism"))
	assert.Equal(t, "--write-batch-bytes", sources.Label("write-batch-bytes"))
}

func TestResolveImport_EnvNames(t *testing.T) {
	env := map[string]string{
		"BYDBCTL_IMPORT_INPUT":               "/in",
		"BYDBCTL_DRY_RUN":                    "true",
		"BYDBCTL_OUTPUT_FORMAT":              "json",
		"BYDBCTL_IMPORT_INCLUDE_SCHEMA":      "false",
		"BYDBCTL_IMPORT_SCHEMA_ONLY":         "true",
		"BYDBCTL_IMPORT_ON_SCHEMA_CONFLICT":  "overwrite",
		"BYDBCTL_IMPORT_PARALLELISM":         "3",
		"BYDBCTL_IMPORT_WRITE_RATE_LIMIT":    "10/s",
		"BYDBCTL_LIAISON_RATE_LIMIT":         "1MiB/s",
		"BYDBCTL_IMPORT_NODE_RATE_LIMIT":     "2MiB/s",
		"BYDBCTL_IMPORT_NODE_ROW_RATE_LIMIT": "20/s",
		// The export's node rates do not reach an import.
		"BYDBCTL_NODE_RATE_LIMIT":               "9MiB/s",
		"BYDBCTL_NODE_ROW_RATE_LIMIT":           "90/s",
		"BYDBCTL_IMPORT_WRITE_BATCH_SIZE":       "10",
		"BYDBCTL_IMPORT_WRITE_BATCH_BYTES":      "128KiB",
		"BYDBCTL_IMPORT_WRITE_INFLIGHT_BATCHES": "8",
		"BYDBCTL_IMPORT_SEND_RETRY_TIMEOUT":     "1m",
		"BYDBCTL_IMPORT_STRICT_COVERAGE":        "true",
		"BYDBCTL_IMPORT_VERIFY":                 "false",
		"BYDBCTL_IMPORT_VERIFY_TIMEOUT":         "30s",
		// The export's variables do not reach an import.
		"BYDBCTL_STRICT_COVERAGE":    "false",
		"BYDBCTL_PARALLELISM":        "9",
		"BYDBCTL_SEND_RETRY_TIMEOUT": "9m",
	}
	imp, _, err := resolveImport(t, boundImport(t), "", env)
	require.NoError(t, err)
	assert.Equal(t, Import{
		Input: "/in", OutputFormat: "json", OnSchemaConflict: ConflictPolicyOverwrite, WriteBatchBytes: 128 << 10,
		WriteRateLimit: 10, LiaisonRateLimit: 1 << 20, NodeRateLimit: 2 << 20, NodeRowRateLimit: 20, SendRetryTimeout: time.Minute, VerifyTimeout: 30 * time.Second,
		Parallelism: 3, WriteBatchSize: 10, WriteInflightBatches: 8, DryRun: true, SchemaOnly: true, StrictCoverage: true,
	}, *imp)
	fs := boundImport(t)
	for _, name := range []string{"force", "force-ttl", "all-sources", "abort-on-plan-change"} {
		assert.NotContains(t, fs.Lookup(name).Usage, "BYDBCTL_", "--%s has no variable", name)
		assert.NotContains(t, fs.Lookup(name).Usage, "plan.yaml", "--%s is not in plan.yaml", name)
	}
	assert.Equal(t, "o", fs.Lookup("output-format").Shorthand)
}

func TestResolveImport_Errors(t *testing.T) {
	for _, c := range []struct {
		env  map[string]string
		plan string
		want string
		args []string
	}{
		{args: []string{"--on-schema-conflict", "Overwrite"}, want: `--on-schema-conflict: unsupported schema conflict policy "Overwrite"`},
		{env: map[string]string{"BYDBCTL_IMPORT_ON_SCHEMA_CONFLICT": "skip"}, want: "BYDBCTL_IMPORT_ON_SCHEMA_CONFLICT: unsupported schema conflict policy"},
		{plan: "import:\n  schemaConflictPolicy:\n    default: true\n", want: "plan.yaml import.schemaConflictPolicy.default: must be a string"},
		{args: []string{"--write-batch-size", "0"}, want: "--write-batch-size: count must be an integer >= 1"},
		{args: []string{"--write-batch-size", "1.5"}, want: "--write-batch-size: count must be"},
		{plan: "import:\n  writeBatchSize: -3\n", want: "plan.yaml import.writeBatchSize: count must be"},
		{plan: "import:\n  writeBatchSize: true\n", want: "plan.yaml import.writeBatchSize: must be a number or a string"},
		{env: map[string]string{"BYDBCTL_IMPORT_WRITE_BATCH_SIZE": "many"}, want: "BYDBCTL_IMPORT_WRITE_BATCH_SIZE: count must be"},
		{args: []string{"--write-rate-limit", "5MiB/s"}, want: "--write-rate-limit: rate \"5MiB/s\": a row rate takes a plain integer"},
		{args: []string{"--node-row-rate-limit", "5MiB/s"}, want: "--node-row-rate-limit: rate \"5MiB/s\": a row rate takes a plain integer"},
		{plan: "import:\n  nodeRateLimit: -1\n", want: "plan.yaml import.nodeRateLimit: rate \"-1\" must not be negative"},
		{plan: "import:\n  schemaConflictPolicy:\n    rules: group/a\n", want: "plan.yaml import.schemaConflictPolicy.rules: must be a list of {match, policy}"},
		{plan: "import:\n  schemaConflictPolicy:\n    rules:\n      - group/a\n", want: "rules: [0]: must be a {match, policy} mapping"},
		{
			plan: rulesPlan("{match: group/a, policy: ignore}", "{match: group/b, policy: ignore, extra: 1}"),
			want: `rules: [1]: must be a {match, policy} mapping: unknown field "extra"`,
		},
		{plan: rulesPlan("{match: group/a, policy: Ignore}"), want: `rules: [0]: unsupported schema conflict policy "Ignore"`},
		{plan: rulesPlan("{match: group/a}"), want: `rules: [0]: unsupported schema conflict policy ""`},
		{plan: rulesPlan("{policy: ignore}"), want: `rules: [0]: match "" must be`},
		{
			plan: rulesPlan("{match: group/a, policy: ignore}", "{match: group/a/b, policy: ignore}"),
			want: "rules: [1]: match \"group/a/b\": the group kind only takes group/<group>",
		},
		{plan: rulesPlan("{match: Measure/g, policy: ignore}"), want: `rules: [0]: match "Measure/g": unknown kind "Measure"`},
		{plan: rulesPlan("{match: measure, policy: ignore}"), want: `rules: [0]: match "measure" must be <kind>/<group>[/<name>]`},
		{plan: rulesPlan("{match: measure/g/n/x, policy: ignore}"), want: "rules: [0]: match \"measure/g/n/x\" must be"},
		{plan: rulesPlan("{match: measure//n, policy: ignore}"), want: "rules: [0]: match \"measure//n\" has an empty segment"},
		{plan: rulesPlan("{match: [a], policy: ignore}"), want: "rules: [0]: match must be a string, got a list"},
		{plan: rulesPlan("{Match: group/a, policy: ignore}"), want: `rules: [0]: must be a {match, policy} mapping: unknown field "Match"`},
		{plan: rulesPlan("{match: group/a, Match: group/b, policy: ignore}"), want: `rules: [0]: must be a {match, policy} mapping: unknown field "Match"`},
		{plan: rulesPlan("{match: group/a, POLICY: ignore}"), want: `rules: [0]: must be a {match, policy} mapping: unknown field "POLICY"`},
	} {
		planFile := ""
		if c.plan != "" {
			planFile = writePlan(t, c.plan)
		}
		_, _, err := resolveImport(t, boundImport(t, c.args...), planFile, c.env)
		require.Error(t, err, c.want)
		assert.Contains(t, err.Error(), c.want)
	}
}

func rulesPlan(rules ...string) string {
	body := "import:\n  schemaConflictPolicy:\n    rules:\n"
	for _, r := range rules {
		body += "      - " + r + "\n"
	}
	return body
}

func TestValidateConflictMatch(t *testing.T) {
	for _, ok := range []string{
		"group/g", "stream/g", "stream/g/n", "measure/g/n", "trace/g/n", "property/g/n",
		"indexRule/g/n", "indexRuleBinding/g/n", "topNAggregation/g/n", "measure/g",
	} {
		assert.NoError(t, ValidateConflictMatch(ok), ok)
	}
	for _, bad := range []string{
		"", "group", "group/", "group/g/n", "indexrule/g/n", "stream/g/", "/g/n", "topn/g/n",
		"node/g", "Measure/g", "measure", "measure/g/n/x", "measure//n",
	} {
		assert.Error(t, ValidateConflictMatch(bad), bad)
	}
}

func TestParseCount(t *testing.T) {
	for raw, want := range map[string]int{"1": 1, "\t1000\n": 1000, "10000": 10000} {
		n, err := ParseCount(raw)
		require.NoError(t, err, raw)
		assert.Equal(t, want, n)
	}
	for _, bad := range []string{"", "0", "-1", "1.0", "1e3", "1k", "max"} {
		_, err := ParseCount(bad)
		assert.Error(t, err, bad)
	}
}

func TestImportValidate(t *testing.T) {
	base := func() *Import {
		return &Import{
			OnSchemaConflict: ConflictPolicyError, WriteBatchBytes: 4 << 20, WriteBatchSize: 1000, WriteInflightBatches: 2,
			IncludeSchema: true, Verify: true, VerifyTimeout: 2 * time.Minute,
		}
	}
	require.NoError(t, base().Validate(Sources{}))

	for _, c := range []struct {
		mutate  func(*Import)
		sources Sources
		want    string
	}{
		{
			mutate: func(i *Import) { i.WriteBatchSize = 10001 }, sources: Sources{"write-batch-size": "--write-batch-size"},
			want: "--write-batch-size: 10001 is outside [1, 10000]",
		},
		{mutate: func(i *Import) { i.WriteBatchSize = 0 }, want: "--write-batch-size: 0 is outside"},
		{
			mutate: func(i *Import) { i.WriteBatchBytes = 64<<10 - 1 }, sources: Sources{"write-batch-bytes": "plan.yaml import.writeBatchBytes"},
			want: "plan.yaml import.writeBatchBytes: 65535 bytes is outside [64KiB, 64MiB]",
		},
		{mutate: func(i *Import) { i.WriteBatchBytes = 64<<20 + 1 }, want: "--write-batch-bytes: 67108865 bytes is outside"},
		{mutate: func(i *Import) { i.DryRun, i.WriteBatchBytes = true, 16<<10 }, want: "--write-batch-bytes: 16384 bytes is outside"},
		{mutate: func(i *Import) { i.SchemaOnly, i.IncludeSchema = true, false }, want: "--schema-only needs the schema; it cannot be combined with --include-schema=false"},
		{mutate: func(i *Import) { i.Verify = false }, sources: Sources{"verify-timeout": "--verify-timeout"}, want: "--verify-timeout has no effect"},
		{
			mutate: func(i *Import) { i.SchemaOnly = true }, sources: Sources{"verify-timeout": "plan.yaml import.verifyTimeout"},
			want: "plan.yaml import.verifyTimeout has no effect",
		},
		{mutate: func(i *Import) { i.VerifyTimeout = 0 }, sources: Sources{"verify-timeout": "--verify-timeout"}, want: "--verify-timeout: 0s must be positive"},
		{mutate: func(i *Import) { i.VerifyTimeout = -time.Second }, want: "--verify-timeout: -1s must be positive"},
		{
			mutate: func(i *Import) { i.WriteInflightBatches = 0 }, sources: Sources{"write-inflight-batches": "plan.yaml import.writeInflightBatches"},
			want: "plan.yaml import.writeInflightBatches: 0 is outside [1, 8]",
		},
		{mutate: func(i *Import) { i.WriteInflightBatches = 9 }, want: "--write-inflight-batches: 9 is outside [1, 8]"},
		{mutate: func(i *Import) { i.Force = true }, want: "--force confirms an overwrite"},
		{
			mutate: func(i *Import) {
				i.Force = true
				i.ConflictRules = []ConflictRule{{Match: "group/g", Policy: ConflictPolicyIgnore}}
			},
			want: "--force confirms an overwrite",
		},
	} {
		imp := base()
		c.mutate(imp)
		sources := c.sources
		if sources == nil {
			sources = Sources{}
		}
		err := imp.Validate(sources)
		require.Error(t, err, c.want)
		assert.Contains(t, err.Error(), c.want)
	}

	for _, c := range []struct {
		mutate  func(*Import)
		sources Sources
		name    string
	}{
		{name: "bounds are inclusive", mutate: func(i *Import) { i.WriteBatchSize, i.WriteBatchBytes, i.WriteInflightBatches = 10000, 64<<20, 8 }},
		{name: "lower bounds are inclusive", mutate: func(i *Import) { i.WriteBatchSize, i.WriteBatchBytes, i.WriteInflightBatches = 1, 64<<10, 1 }},
		{
			name: "verify-timeout from the environment is unused", mutate: func(i *Import) { i.Verify = false },
			sources: Sources{"verify-timeout": "BYDBCTL_IMPORT_VERIFY_TIMEOUT"},
		},
		{name: "dry run ignores verify-timeout", mutate: func(i *Import) { i.Verify, i.DryRun = false, true }, sources: Sources{"verify-timeout": "--verify-timeout"}},
		{name: "dry run ignores write-batch-size", mutate: func(i *Import) { i.DryRun, i.WriteBatchSize = true, 20000 }},
		{name: "dry run ignores write-inflight-batches", mutate: func(i *Import) { i.DryRun, i.WriteInflightBatches = true, 0 }},
		{name: "a short verify-timeout", mutate: func(i *Import) { i.VerifyTimeout = time.Second }},
		{name: "verify off skips the verify-timeout check", mutate: func(i *Import) { i.Verify, i.VerifyTimeout = false, 0 }},
		{name: "dry run skips the verify-timeout check", mutate: func(i *Import) { i.DryRun, i.VerifyTimeout = true, 0 }},
		{name: "schema only skips the verify-timeout check", mutate: func(i *Import) { i.SchemaOnly, i.VerifyTimeout = true, 0 }},
		{name: "force with an overwrite default", mutate: func(i *Import) { i.Force, i.OnSchemaConflict = true, ConflictPolicyOverwrite }},
		{
			name: "force with an overwrite rule",
			mutate: func(i *Import) {
				i.Force = true
				i.ConflictRules = []ConflictRule{{Match: "group/a", Policy: ConflictPolicyIgnore}, {Match: "measure/b/c", Policy: ConflictPolicyOverwrite}}
			},
		},
	} {
		imp := base()
		c.mutate(imp)
		sources := c.sources
		if sources == nil {
			sources = Sources{}
		}
		assert.NoError(t, imp.Validate(sources), c.name)
	}
}

func TestImportEffectiveVerify(t *testing.T) {
	assert.True(t, (&Import{Verify: true}).EffectiveVerify())
	assert.False(t, (&Import{Verify: false}).EffectiveVerify())
	assert.False(t, (&Import{Verify: true, SchemaOnly: true}).EffectiveVerify(), "--schema-only turns verify off")
	assert.False(t, (&Import{Verify: true, DryRun: true}).EffectiveVerify(), "a dry run writes nothing to verify")
}

func TestResolveImport_ValidateEndToEnd(t *testing.T) {
	imp, sources, err := resolveImport(t, boundImport(t, "--verify=false"), writePlan(t, "import:\n  verifyTimeout: 1m\n"), nil)
	require.NoError(t, err)
	err = imp.Validate(sources)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan.yaml import.verifyTimeout has no effect")

	imp, sources, err = resolveImport(t, boundImport(t, "--force"), writePlan(t, rulesPlan("{match: group/a, policy: overwrite}")), nil)
	require.NoError(t, err)
	require.NoError(t, imp.Validate(sources))

	imp, sources, err = resolveImport(t, boundImport(t, "--force", "--write-batch-bytes", "16KiB"), "", nil)
	require.NoError(t, err)
	err = imp.Validate(sources)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--write-batch-bytes: 16384 bytes is outside")
}
