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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// bound returns a flag set with Connection and Export bound, parsed from args.
func bound(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	Bind(fs, &Connection{}, &Export{})
	require.NoError(t, fs.Parse(args))
	return fs
}

func writePlan(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func envOf(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}
}

func resolve(t *testing.T, fs *pflag.FlagSet, planFile string, env map[string]string) (*Connection, *Export, Sources, error) {
	t.Helper()
	if planFile != "" {
		require.NoError(t, fs.Set(PlanFlag, planFile))
	}
	conn, exp := &Connection{}, &Export{}
	sources, err := Resolve(fs, envOf(env), conn, exp)
	return conn, exp, sources, err
}

func TestResolve_Defaults(t *testing.T) {
	conn, exp, sources, err := resolve(t, bound(t), "", nil)
	require.NoError(t, err)
	assert.Empty(t, sources)
	assert.Equal(t, Connection{LoggingLevel: "info", LoggingEnv: "prod"}, *conn)
	assert.Equal(t, Export{
		OutputFormat: "table", MaxFileSize: 512 << 20, NodeIdleTimeout: 10 * time.Minute, SendRetryTimeout: 15 * time.Minute,
		Parallelism: math.MaxInt, Format: transferv1.Format_FORMAT_CSV, Compress: transferv1.Compress_COMPRESS_GZIP, IncludeSchema: true,
	}, *exp)
}

// dryRunPlanYAML is the dry-run subset of a full plan.yaml.
const dryRunPlanYAML = `
connection:
  nodes: [liaison-1:17912, liaison-2:17912]
  nodesTLS:  {enable: true, insecure: true, cert: "/c.pem"}
export:
  parallelism: max
  selectors:
    - catalog: stream
      groups: [sw_record, sw_log]
    - catalog: measure
      groups: [sw_metric]
    - catalog: property
`

func TestResolve_DryRunPlan(t *testing.T) {
	conn, exp, sources, err := resolve(t, bound(t), writePlan(t, dryRunPlanYAML), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"liaison-1:17912", "liaison-2:17912"}, conn.Nodes)
	assert.True(t, conn.EnableTLS)
	assert.True(t, conn.Insecure)
	assert.Equal(t, "/c.pem", conn.Cert)
	assert.Equal(t, math.MaxInt, exp.Parallelism)
	require.Len(t, exp.Selectors, 3)
	assert.Equal(t, commonv1.Catalog_CATALOG_STREAM, exp.Selectors[0].GetCatalog())
	assert.Equal(t, []string{"sw_record", "sw_log"}, exp.Selectors[0].GetGroups())
	assert.Equal(t, commonv1.Catalog_CATALOG_PROPERTY, exp.Selectors[2].GetCatalog())
	assert.Empty(t, exp.Selectors[2].GetGroups())
	assert.True(t, sources.FromPlan("nodes"))
	assert.Equal(t, "plan.yaml export.selectors", sources.Label("selector"))
}

// exportPlanYAML carries every export field of plan.yaml.
const exportPlanYAML = `
connection:
  nodes: [liaison-1:17912]
  rateLimit: "800MiB/s"
export:
  output: ./backup-20260708
  format: CSV
  compress: none
  maxFileSize: 512MiB
  includeSchema: false
  schemaOnly: true
  csvAnnotations: true
  parallelism: 2
  nodeRateLimit: 52428800
  nodeRowRateLimit: "3000000/min"
  nodeIdleTimeout: 30s
  sendRetryTimeout: 1h
`

func TestResolve_ExportPlan(t *testing.T) {
	_, exp, _, err := resolve(t, bound(t), writePlan(t, exportPlanYAML), nil)
	require.NoError(t, err)
	assert.Equal(t, Export{
		Output: "./backup-20260708", OutputFormat: "table", MaxFileSize: 512 << 20,
		NodeRateLimit: 50 << 20, NodeRowRateLimit: 50000, LiaisonRateLimit: 800 << 20,
		NodeIdleTimeout: 30 * time.Second, SendRetryTimeout: time.Hour, Parallelism: 2,
		Format: transferv1.Format_FORMAT_CSV, Compress: transferv1.Compress_COMPRESS_NONE,
		CSVAnnotations: true, SchemaOnly: true,
	}, *exp)
}

func TestResolve_Precedence(t *testing.T) {
	planFile := writePlan(t, "export:\n  compress: none\n  maxFileSize: 1KiB\n")
	env := map[string]string{"BYDBCTL_COMPRESS": "gzip", "BYDBCTL_MAX_FILE_SIZE": "2KiB", "BYDBCTL_OUTPUT": "/env", "BYDBCTL_PARALLELISM": ""}

	_, exp, sources, err := resolve(t, bound(t, "--max-file-size", "3KiB"), planFile, env)
	require.NoError(t, err)
	assert.Equal(t, uint64(3<<10), exp.MaxFileSize, "an explicit flag beats plan.yaml")
	assert.Equal(t, transferv1.Compress_COMPRESS_NONE, exp.Compress, "plan.yaml beats the environment")
	assert.Equal(t, "/env", exp.Output, "the environment beats the default")
	assert.Equal(t, math.MaxInt, exp.Parallelism, "an empty variable counts as unset")
	assert.True(t, sources.FromFlag("max-file-size"))
	assert.True(t, sources.FromPlan("compress"))
	assert.Equal(t, "BYDBCTL_OUTPUT", sources.Label("output"))
	assert.Equal(t, "--parallelism", sources.Label("parallelism"), "a default is labeled by its flag")

	// An explicit false flag beats a true plan.yaml value.
	_, exp, _, err = resolve(t, bound(t, "--schema-only=false"), writePlan(t, "export:\n  schemaOnly: true\n"), nil)
	require.NoError(t, err)
	assert.False(t, exp.SchemaOnly)
}

func TestResolve_EnvLists(t *testing.T) {
	conn, _, _, err := resolve(t, bound(t), "", map[string]string{"BYDBCTL_NODES": " a:1, ,b:2 "})
	require.NoError(t, err)
	assert.Equal(t, []string{"a:1", "b:2"}, conn.Nodes)
	// The flag's list is trimmed the same way.
	conn, _, _, err = resolve(t, bound(t, "--nodes", "c:3, d:4,"), "", map[string]string{"BYDBCTL_NODES": "a:1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"c:3", "d:4"}, conn.Nodes)
}

func TestResolve_ErrorsNameTheSource(t *testing.T) {
	for _, c := range []struct {
		env  map[string]string
		plan string
		want string
		args []string
	}{
		{args: []string{"--node-rate-limit", "50MiB/m"}, want: `--node-rate-limit: rate "50MiB/m": period "m" is ambiguous`},
		{plan: "export:\n  nodeRateLimit: -1\n", want: "plan.yaml export.nodeRateLimit: rate \"-1\" must not be negative"},
		{env: map[string]string{"BYDBCTL_NODE_ROW_RATE_LIMIT": "5MiB"}, want: "BYDBCTL_NODE_ROW_RATE_LIMIT: rate \"5MiB\": a row rate takes a plain integer"},
		{args: []string{"--max-file-size", "-1"}, want: "--max-file-size: size \"-1\" must not be negative"},
		{args: []string{"--parallelism", "0"}, want: "--parallelism: parallelism must be"},
		{plan: "export:\n  parallelism: 0\n", want: "plan.yaml export.parallelism: parallelism must be"},
		{env: map[string]string{"BYDBCTL_PARALLELISM": "0"}, want: "BYDBCTL_PARALLELISM: parallelism must be"},
		{args: []string{"--node-idle-timeout", "-1s"}, want: "--node-idle-timeout: duration \"-1s\" must not be negative"},
		{plan: "export:\n  sendRetryTimeout: 5\n", want: "plan.yaml export.sendRetryTimeout: time: missing unit in duration"},
		{plan: "export:\n  includeSchema: \"no\"\n", want: "plan.yaml export.includeSchema: must be true or false"},
		{env: map[string]string{"BYDBCTL_PREEMPT": "maybe"}, want: "BYDBCTL_PREEMPT: \"maybe\" is not a boolean"},
		{args: []string{"--compress", "zstd"}, want: `--compress: unsupported compress "zstd" (none|gzip)`},
		{env: map[string]string{"BYDBCTL_FORMAT": "parquet"}, want: `BYDBCTL_FORMAT: unsupported format "parquet"`},
		{args: []string{"--selector", "catalog=stream,foo=bar"}, want: "--selector: selector"},
		{args: []string{"--selector", "catalog=stream", "--selector", "catalog=stream,groups=a;a"}, want: `--selector: [1]: group "a" given twice`},
		{plan: "export:\n  selectors:\n    - catalog: stream\n      groups: [\"\"]\n", want: "plan.yaml export.selectors: [0]: empty group name"},
		{plan: "export:\n  selectors:\n    - catalog: stream\n      group: [a]\n", want: "plan.yaml export.selectors: must be a list of {catalog, groups}"},
		{plan: "connection:\n  nodes: liaison:17912\n", want: "plan.yaml connection.nodes: must be a list of strings"},
	} {
		planFile := ""
		if c.plan != "" {
			planFile = writePlan(t, c.plan)
		}
		_, _, _, err := resolve(t, bound(t, c.args...), planFile, c.env)
		require.Error(t, err, c.want)
		assert.Contains(t, err.Error(), c.want)
	}
}

func TestLoadPlan_RejectsUnknownFields(t *testing.T) {
	// A misspelled field, and the plan fields that only a resumed export or an import reads
	// (they land with those steps), are all unknown to the strict parser.
	for _, body := range []string{
		"connection:\n  nodes: [a:17912]\nexport:\n  outpt: ./x\n",
		"connection:\n  nodes: [a:17912]\nexport:\n  checkpoint: ./x\n",
		"connection:\n  nodes: [a:17912]\nexport:\n  archive: tar\n",
		"connection:\n  nodes: [a:17912]\nimport:\n  input: ./x\n",
		"connection:\n  nodesTLS:\n    verify: true\n",
		"export:\n  preempt: true\n",
		"export:\n  dryRun: true\n",
		"connection: [a]\n",
		"- a\n",
		// A key given twice would silently drop the first value.
		"export:\n  output: ./x\nexport:\n  parallelism: 3\n",
		"export:\n  parallelism: 4\n  parallelism: 8\n",
	} {
		_, err := LoadPlan(writePlan(t, body))
		require.Error(t, err, "must be rejected:\n%s", body)
		assert.Contains(t, err.Error(), "parse plan file")
	}
	_, err := LoadPlan(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err, "a missing file must be reported")
	leaves, err := LoadPlan(writePlan(t, "export:\nconnection:\n  nodes:\n"))
	require.NoError(t, err, "empty sections and null values are absent")
	assert.Empty(t, leaves)
}

func TestResolve_PlanDurationZero(t *testing.T) {
	_, exp, sources, err := resolve(t, bound(t), writePlan(t, "export:\n  nodeIdleTimeout: 0\n"), nil)
	require.NoError(t, err, "0 disables the idle timeout in plan.yaml as on the command line")
	assert.Zero(t, exp.NodeIdleTimeout)
	assert.True(t, sources.FromPlan("node-idle-timeout"))
	_, _, sources, err = resolve(t, bound(t), writePlan(t, "export:\n  nodeIdleTimeout: 10\n"), nil)
	require.Error(t, err, "a bare non-zero number has no unit")
	assert.Nil(t, sources)
}

func TestBind_UsageNamesPlanKeyAndVariable(t *testing.T) {
	fs := bound(t)
	usage := fs.Lookup("node-rate-limit").Usage
	assert.True(t, strings.HasSuffix(usage, "(plan.yaml export.nodeRateLimit, BYDBCTL_NODE_RATE_LIMIT)"), usage)
	assert.True(t, strings.HasSuffix(fs.Lookup("preempt").Usage, "(BYDBCTL_PREEMPT)"))
	assert.Equal(t, "o", fs.Lookup("output-format").Shorthand)
	assert.Equal(t, "true", fs.Lookup("include-schema").DefValue)
	assert.Equal(t, "10m0s", fs.Lookup("node-idle-timeout").DefValue)
	assert.Equal(t, "512MiB", fs.Lookup("max-file-size").DefValue)
}

func TestParseParallelism(t *testing.T) {
	if n, err := ParseParallelism("max"); err != nil || n != math.MaxInt {
		t.Fatalf("max -> %d, %v", n, err)
	}
	if n, err := ParseParallelism("4"); err != nil || n != 4 {
		t.Fatalf("4 -> %d, %v", n, err)
	}
	for _, bad := range []string{"0", "-1", "", "fast", "1.5"} {
		if _, err := ParseParallelism(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
	if got, clamped := ClampParallelism(10, 5); got != 5 || !clamped {
		t.Fatalf("clamp(10,5) = %d,%v", got, clamped)
	}
	if got, clamped := ClampParallelism(3, 5); got != 3 || clamped {
		t.Fatalf("clamp(3,5) = %d,%v", got, clamped)
	}
	if got, _ := ClampParallelism(math.MaxInt, 0); got != 1 {
		t.Fatalf("clamp with no nodes = %d, want 1", got)
	}
}
