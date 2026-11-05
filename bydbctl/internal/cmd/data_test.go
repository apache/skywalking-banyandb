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
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// resolveConnection resolves a data command's connection from args and planFile.
func resolveConnection(t *testing.T, planFile string, args ...string) (*config.Connection, error) {
	t.Helper()
	c := &cobra.Command{Use: "x"}
	conn := &config.Connection{}
	config.Bind(c.Flags(), conn)
	if planFile != "" {
		args = append(args, "--"+config.PlanFlag, planFile)
	}
	require.NoError(t, c.ParseFlags(args))
	_, err := resolveSettings(c, conn)
	return conn, err
}

func TestResolveSettings_NodesTLSKeepsExplicitFlags(t *testing.T) {
	planFile := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(planFile, []byte("connection:\n  nodes: [a:17912]\n  nodesTLS: {enable: true, insecure: true, cert: /etc/plan.crt}\n"), 0o600))

	conn, err := resolveConnection(t, planFile, "--cert", "/etc/flag.crt")
	require.NoError(t, err)
	assert.True(t, conn.EnableTLS, "enable-tls was not given: plan.yaml decides")
	assert.True(t, conn.Insecure, "insecure was not given: plan.yaml decides")
	assert.Equal(t, "/etc/flag.crt", conn.Cert, "an explicit --cert must not be clobbered by plan.yaml")

	conn, err = resolveConnection(t, planFile, "--insecure=false", "--enable-tls=false")
	require.NoError(t, err)
	assert.False(t, conn.EnableTLS, "an explicit --enable-tls=false wins over plan.yaml")
	assert.False(t, conn.Insecure, "an explicit --insecure=false wins over plan.yaml")
	assert.Equal(t, "/etc/plan.crt", conn.Cert)

	_, err = resolveConnection(t, "")
	requireUsage(t, err, "no liaison address")
}

// releaseLiaison is a liaison with data nodes data-a and data-b whose release answers frames.
type releaseLiaison struct {
	databasev1.UnimplementedNodeQueryServiceServer
	databasev1.UnimplementedClusterStateServiceServer
	transferv1.UnimplementedExportServiceServer
	frames []*transferv1.SessionsResponse
}

func (releaseLiaison) GetCurrentNode(context.Context, *databasev1.GetCurrentNodeRequest) (*databasev1.GetCurrentNodeResponse, error) {
	return &databasev1.GetCurrentNodeResponse{Node: &databasev1.Node{
		Metadata: &commonv1.Metadata{Name: "liaison-1"}, Roles: []databasev1.Role{databasev1.Role_ROLE_LIAISON},
	}}, nil
}

func (releaseLiaison) GetClusterState(context.Context, *databasev1.GetClusterStateRequest) (*databasev1.GetClusterStateResponse, error) {
	var registered []*databasev1.Node
	for _, n := range []string{"data-a", "data-b"} {
		registered = append(registered, &databasev1.Node{Metadata: &commonv1.Metadata{Name: n}, Roles: []databasev1.Role{databasev1.Role_ROLE_DATA}})
	}
	return &databasev1.GetClusterStateResponse{RouteTables: map[string]*databasev1.RouteTable{"tire2": {Registered: registered}}}, nil
}

func (l releaseLiaison) Sessions(_ *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	for _, f := range l.frames {
		if err := stream.Send(f); err != nil {
			return err
		}
	}
	return nil
}

func releaseFrame(node, outcome string) *transferv1.SessionsResponse {
	f := &transferv1.SessionsResponse{NodeId: node}
	switch outcome {
	case "done":
		f.Outcome = &transferv1.SessionsResponse_Done{Done: &transferv1.Ack{}}
	case "none":
		f.Outcome = &transferv1.SessionsResponse_None{None: &transferv1.Ack{}}
	default:
		f.Outcome = &transferv1.SessionsResponse_Error{Error: outcome}
	}
	return f
}

// runData executes `bydbctl <args>` on a fresh root, the way main does, so initConfig sees a
// data command. HOME points at an empty directory, which must still hold no config file
// afterwards: data commands neither read nor create it.
func runData(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	out, errOut, err := executeOnFreshRoot(t, args...)
	_, statErr := os.Stat(filepath.Join(home, ".bydbctl.yaml"))
	require.True(t, os.IsNotExist(statErr), "a data command must not create the config file: %v", statErr)
	return out, errOut, err
}

// executeOnFreshRoot runs args on a new root command and restores the package root afterwards.
func executeOnFreshRoot(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	ResetFlags()
	savedRoot := activeRoot
	t.Cleanup(func() {
		activeRoot = savedRoot
		SetTestArgs(nil)
		ResetFlags()
	})
	root := &cobra.Command{Use: "root"}
	RootCmdFlags(root)
	SetTestArgs(args)
	root.SetArgs(args)
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	err := root.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

// serveRelease starts a liaison whose release answers frames and returns its address.
func serveRelease(t *testing.T, frames ...*transferv1.SessionsResponse) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	fake := releaseLiaison{frames: frames}
	databasev1.RegisterNodeQueryServiceServer(srv, fake)
	databasev1.RegisterClusterStateServiceServer(srv, fake)
	transferv1.RegisterExportServiceServer(srv, fake)
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// runRelease runs `data release-session --id abcd` against a liaison answering frames.
func runRelease(t *testing.T, frames ...*transferv1.SessionsResponse) (string, string, error) {
	t.Helper()
	return runData(t, "data", "release-session", "--id", "abcdabcdabcdabcdabcdabcdabcdabcd", "--nodes", serveRelease(t, frames...))
}

// Data commands never look up the home directory, so they run with HOME unset.
func TestReleaseSession_RunsWithoutHome(t *testing.T) {
	addr := serveRelease(t, releaseFrame("data-a", "done"))
	t.Setenv("HOME", "")
	out, _, err := executeOnFreshRoot(t, "data", "release-session", "--id", "abcdabcdabcdabcdabcdabcdabcdabcd", "--nodes", addr)
	require.NoError(t, err)
	assert.Equal(t, "export session abcdabcdabcdabcdabcdabcdabcdabcd released\n", out)
}

func TestReleaseSession_ReportsPartialFailure(t *testing.T) {
	out, errOut, err := runRelease(t, releaseFrame("data-a", "done"), releaseFrame("data-b", "disk busy"))
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exitcode.ExitRuntime, exitErr.Code, "a partial release must exit non-zero")
	assert.Contains(t, err.Error(), "released except on [data-b]")
	assert.Equal(t, "release failed on data-b: disk busy\n", errOut, "one line per failed node")
	assert.Empty(t, out, "a partial release must not claim success")
}

func TestReleaseSession_ReportsTotalFailure(t *testing.T) {
	out, _, err := runRelease(t, releaseFrame("data-a", "disk busy"), releaseFrame("data-b", "none"))
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exitcode.ExitRuntime, exitErr.Code)
	assert.Contains(t, err.Error(), "session abcdabcdabcdabcdabcdabcdabcdabcd could not be released on [data-a]")
	assert.NotContains(t, err.Error(), "released except on", "nothing was released")
	assert.Empty(t, out)
}

func TestReleaseSession_UnknownSessionIsAnError(t *testing.T) {
	out, _, err := runRelease(t, releaseFrame("data-a", "none"), releaseFrame("data-b", "none"))
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exitcode.ExitPreflight, exitErr.Code, "a session no node holds is a preflight rejection")
	assert.Contains(t, err.Error(), "export session abcdabcdabcdabcdabcdabcdabcdabcd is not held on any data node")
	assert.Empty(t, out, "nothing was released")
}

func TestReleaseSession_NotesNodesThatDidNotHoldIt(t *testing.T) {
	out, errOut, err := runRelease(t, releaseFrame("data-a", "done"), releaseFrame("data-b", "none"))
	require.NoError(t, err)
	assert.Equal(t, "export session abcdabcdabcdabcdabcdabcdabcdabcd released\n", out)
	assert.Equal(t, "export session abcdabcdabcdabcdabcdabcdabcdabcd was not held on [data-b]\n", errOut)
}

func TestDataExport_RejectsPositionalArgs(t *testing.T) {
	_, _, err := runData(t, "data", "export", "extra", "--dry-run", "--nodes", "127.0.0.1:1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown command "extra"`)
}

func TestDataExport_SelectorRejectsDuplicateGroup(t *testing.T) {
	// 127.0.0.1:1 never answers: the selector must be rejected before the preflight.
	c := mustParsed(t, "--dry-run", "--nodes", "127.0.0.1:1", "--selector", "catalog=stream,groups=a;a")
	err := c.PreRunE(c, nil)
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exitcode.ExitUsage, exitErr.Code)
	assert.Contains(t, err.Error(), `--selector: [0]: group "a" given twice`)
}

// preRunExport parses args on a fresh `data export` command and runs its PreRunE only, so
// nothing is contacted; 127.0.0.1:1 would never answer anyway.
func preRunExport(t *testing.T, args ...string) error {
	t.Helper()
	c := mustParsed(t, append([]string{"--nodes", "127.0.0.1:1"}, args...)...)
	return c.PreRunE(c, nil)
}

func requireUsage(t *testing.T, err error, contains string) {
	t.Helper()
	var exitErr *exitcode.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exitcode.ExitUsage, exitErr.Code, "%v", err)
	assert.Contains(t, err.Error(), contains)
}

func TestDataExport_FlagValidation(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	for _, c := range []struct {
		want string
		args []string
	}{
		{"--output cannot be combined with --dry-run", []string{"--dry-run", "--output", out}},
		{"--preempt cannot be combined with --dry-run", []string{"--dry-run", "--preempt"}},
		{"--schema-only cannot be combined with --dry-run", []string{"--dry-run", "--schema-only"}},
		{"native export format is not implemented yet", []string{"--output", out, "--format", "native"}},
		{"native export format is not implemented yet", []string{"--dry-run", "--format", "native"}},
		{"unsupported compress", []string{"--dry-run", "--compress", "zstd"}},
		{"--liaison-rate-limit", []string{"--dry-run", "--liaison-rate-limit", "fast"}},
		{"unsupported format", []string{"--output", out, "--format", "parquet"}},
		{"unsupported compress", []string{"--output", out, "--compress", "zstd"}},
		{"--max-file-size", []string{"--output", out, "--max-file-size", "-1"}},
		{"--schema-only needs the schema; it cannot be combined with --include-schema=false", []string{"--output", out, "--schema-only", "--include-schema=false"}},
		{"--node-rate-limit", []string{"--output", out, "--node-rate-limit", "-1"}},
		{`use "min"`, []string{"--output", out, "--node-rate-limit", "50MiB/m"}},
		{"not a byte unit", []string{"--output", out, "--node-row-rate-limit", "50MiB/s"}},
		{"below 1", []string{"--output", out, "--liaison-rate-limit", "1KiB/h"}},
		{"no output directory", nil},
		{"--strict-coverage applies to --dry-run only", []string{"--output", out, "--strict-coverage"}},
		{"must not be negative", []string{"--output", out, "--node-idle-timeout", "-1s"}},
	} {
		err := preRunExport(t, c.args...)
		requireUsage(t, err, c.want)
	}
}

func TestDataExport_OutputMustBeNewOrEmpty(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "leftover"), nil, 0o600))
	err := preRunExport(t, "--output", dir)
	requireUsage(t, err, "is not empty")
	err = preRunExport(t, "--output", t.TempDir())
	require.NoError(t, err, "an empty directory is accepted")
	err = preRunExport(t, "--output", filepath.Join(t.TempDir(), "new"), "--format", "CSV", "--compress", "none",
		"--node-rate-limit", "3GiB/min", "--node-row-rate-limit", "3000000/min", "--liaison-rate-limit", "52428800", "--max-file-size", "0")
	require.NoError(t, err)
}

// plan.yaml settles every export setting the flags leave alone, and BYDBCTL_* comes last.
func TestDataExport_PlanFilePrecedence(t *testing.T) {
	dir := t.TempDir()
	leftover := filepath.Join(dir, "busy")
	require.NoError(t, os.MkdirAll(leftover, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(leftover, "x"), nil, 0o600))
	planFile := filepath.Join(dir, "plan.yaml")
	body := "connection:\n  rateLimit: \"1KiB/h\"\nexport:\n  output: " + leftover + "\n  compress: none\n  nodeRowRateLimit: 50MiB/s\n  includeSchema: false\n"
	require.NoError(t, os.WriteFile(planFile, []byte(body), 0o600))
	t.Setenv("BYDBCTL_COMPRESS", "zstd")

	err := preRunExport(t, "--plan", planFile)
	requireUsage(t, err, "plan.yaml export.nodeRowRateLimit")
	err = preRunExport(t, "--plan", planFile, "--node-row-rate-limit", "0")
	requireUsage(t, err, "plan.yaml connection.rateLimit")
	err = preRunExport(t, "--plan", planFile, "--node-row-rate-limit", "0", "--liaison-rate-limit", "0")
	requireUsage(t, err, "is not empty")
	fresh := filepath.Join(dir, "fresh")
	err = preRunExport(t, "--plan", planFile, "--node-row-rate-limit", "0", "--liaison-rate-limit", "0", "--output", fresh)
	require.NoError(t, err, "plan.yaml compress none beats BYDBCTL_COMPRESS")

	schemaPlan := filepath.Join(dir, "schema.yaml")
	require.NoError(t, os.WriteFile(schemaPlan, []byte("connection:\n  nodes: [a:17912]\nexport:\n  compress: none\n  includeSchema: false\n  schemaOnly: true\n"), 0o600))
	c := mustParsed(t, "--output", fresh, "--include-schema", "--plan", schemaPlan)
	opts := &config.Export{}
	_, err = resolveSettings(c, &config.Connection{}, opts)
	require.NoError(t, err)
	assert.Equal(t, transferv1.Compress_COMPRESS_NONE, opts.Compress)
	assert.True(t, opts.IncludeSchema, "an explicit flag beats plan.yaml")
	assert.True(t, opts.SchemaOnly, "plan.yaml decides an unset flag")
	assert.Equal(t, uint64(512<<20), opts.MaxFileSize)
	err = preRunExport(t, "--plan", schemaPlan, "--output", fresh)
	requireUsage(t, err, "plan.yaml export.schemaOnly needs the schema; it cannot be combined with plan.yaml export.includeSchema=false")

	err = preRunExport(t, "--plan", schemaPlan, "--dry-run")
	requireUsage(t, err, "plan.yaml export.schemaOnly cannot be combined with --dry-run")

	err = preRunExport(t, "--output", filepath.Join(dir, "env"))
	requireUsage(t, err, `unsupported compress "zstd"`)
}

// mustParsed parses args on a fresh `data export` command.
func mustParsed(t *testing.T, args ...string) *cobra.Command {
	t.Helper()
	c, _ := parseDataCmd(t, newDataExportCmd, args...)
	return c
}

// parseDataCmd parses args on a fresh command built by newCmd, with the global flags reset
// before and after the test, and returns the command with its captured stderr.
func parseDataCmd(t *testing.T, newCmd func() *cobra.Command, args ...string) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	ResetFlags()
	t.Cleanup(ResetFlags)
	c := newCmd()
	var errOut bytes.Buffer
	c.SetErr(&errOut)
	require.NoError(t, c.ParseFlags(args))
	return c, &errOut
}
