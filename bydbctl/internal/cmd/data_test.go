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
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

func TestMergeNodesTLS_KeepsExplicitFlags(t *testing.T) {
	t.Cleanup(ResetFlags)
	fromPlan := transfer.TLSConfig{Enable: true, Insecure: true, Cert: "/etc/plan.crt"}

	c := &cobra.Command{Use: "x"}
	bindTLSRelatedFlag(c)
	require.NoError(t, c.ParseFlags([]string{"--cert", "/etc/flag.crt"}))
	mergeNodesTLS(c, fromPlan)
	assert.True(t, enableTLS, "enable-tls was not given: plan.yaml decides")
	assert.True(t, insecure, "insecure was not given: plan.yaml decides")
	assert.Equal(t, "/etc/flag.crt", cert, "an explicit --cert must not be clobbered by plan.yaml")

	ResetFlags()
	c = &cobra.Command{Use: "x"}
	bindTLSRelatedFlag(c)
	require.NoError(t, c.ParseFlags([]string{"--insecure=false", "--enable-tls=false"}))
	mergeNodesTLS(c, fromPlan)
	assert.False(t, enableTLS, "an explicit --enable-tls=false wins over plan.yaml")
	assert.False(t, insecure, "an explicit --insecure=false wins over plan.yaml")
	assert.Equal(t, "/etc/plan.crt", cert)
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
	return runData(t, "data", "release-session", "--id", "abcd", "--nodes", serveRelease(t, frames...))
}

// Data commands never look up the home directory, so they run with HOME unset.
func TestReleaseSession_RunsWithoutHome(t *testing.T) {
	addr := serveRelease(t, releaseFrame("data-a", "done"))
	t.Setenv("HOME", "")
	out, _, err := executeOnFreshRoot(t, "data", "release-session", "--id", "abcd", "--nodes", addr)
	require.NoError(t, err)
	assert.Equal(t, "export session abcd released\n", out)
}

func TestReleaseSession_ReportsPartialFailure(t *testing.T) {
	out, errOut, err := runRelease(t, releaseFrame("data-a", "done"), releaseFrame("data-b", "disk busy"))
	var exitErr *exporter.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exporter.ExitRuntime, exitErr.Code, "a partial release must exit non-zero")
	assert.Contains(t, err.Error(), "released except on [data-b]")
	assert.Equal(t, "release failed on data-b: disk busy\n", errOut, "one line per failed node")
	assert.Empty(t, out, "a partial release must not claim success")
}

func TestReleaseSession_ReportsTotalFailure(t *testing.T) {
	out, _, err := runRelease(t, releaseFrame("data-a", "disk busy"), releaseFrame("data-b", "none"))
	var exitErr *exporter.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exporter.ExitRuntime, exitErr.Code)
	assert.Contains(t, err.Error(), "session abcd could not be released on [data-a]")
	assert.NotContains(t, err.Error(), "released except on", "nothing was released")
	assert.Empty(t, out)
}

func TestReleaseSession_UnknownSessionIsAnError(t *testing.T) {
	out, _, err := runRelease(t, releaseFrame("data-a", "none"), releaseFrame("data-b", "none"))
	var exitErr *exporter.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exporter.ExitPreflight, exitErr.Code, "a session no node holds is a preflight rejection")
	assert.Contains(t, err.Error(), "export session abcd is not held on any data node")
	assert.Empty(t, out, "nothing was released")
}

func TestReleaseSession_NotesNodesThatDidNotHoldIt(t *testing.T) {
	out, errOut, err := runRelease(t, releaseFrame("data-a", "done"), releaseFrame("data-b", "none"))
	require.NoError(t, err)
	assert.Equal(t, "export session abcd released\n", out)
	assert.Equal(t, "export session abcd was not held on [data-b]\n", errOut)
}

func TestDataExport_RejectsPositionalArgs(t *testing.T) {
	_, _, err := runData(t, "data", "export", "extra", "--dry-run", "--nodes", "127.0.0.1:1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown command "extra"`)
}

func TestDataExport_SelectorRejectsDuplicateGroup(t *testing.T) {
	ResetFlags()
	t.Cleanup(ResetFlags)
	c := newDataExportCmd()
	// 127.0.0.1:1 never answers: the selector must be rejected before the preflight.
	require.NoError(t, c.ParseFlags([]string{"--dry-run", "--nodes", "127.0.0.1:1", "--selector", "catalog=stream,groups=a;a"}))
	err := c.PreRunE(c, nil)
	var exitErr *exporter.ExitError
	require.True(t, errors.As(err, &exitErr), "want an exit error, got %v", err)
	assert.Equal(t, exporter.ExitUsage, exitErr.Code)
	assert.Contains(t, err.Error(), `--selector[0]: group "a" given twice`)
}
