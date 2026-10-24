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

package exporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ClusterInfo is what the connection preflight learned about the source cluster from the
// primary liaison.
type ClusterInfo struct {
	Liaison    string
	DataNodes  []*databasev1.Node
	Standalone bool
}

// ConnectOptions are the dial parameters shared by every liaison candidate.
type ConnectOptions struct {
	// Log receives a WARN line for every candidate that is skipped; nil discards them.
	Log       io.Writer
	Cert      string
	Username  string
	Password  string
	Timeout   time.Duration
	EnableTLS bool
	Insecure  bool
}

// Liaison is one usable liaison: its address and its connection.
type Liaison struct {
	Conn *grpc.ClientConn
	Addr string
}

// Liaisons are the usable liaisons in the order they were given. The first is the primary:
// the schema, Plan and Sessions calls go through it, while export streams are spread over
// all of them.
type Liaisons struct {
	All []*Liaison
}

// Primary is the connection of the first usable liaison.
func (l *Liaisons) Primary() *grpc.ClientConn { return l.All[0].Conn }

// Close closes every connection.
func (l *Liaisons) Close() error {
	var errs []error
	for _, li := range l.All {
		errs = append(errs, li.Conn.Close())
	}
	return errors.Join(errs...)
}

// basicCredentials attaches the username/password metadata the liaison's auth interceptor
// reads to every RPC on the connection, not only to the health check.
type basicCredentials struct {
	username, password string
	secure             bool
}

func (c basicCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"username": c.username, "password": c.password}, nil
}

func (c basicCredentials) RequireTransportSecurity() bool { return c.secure }

var _ credentials.PerRPCCredentials = basicCredentials{}

// describeTimeout bounds one candidate's describe RPCs so a hung liaison does not hold up
// the preflight.
var describeTimeout = 10 * time.Second

// errNoDataNode marks a liaison that answered but has not discovered any data node yet.
var errNoDataNode = errors.New("has not discovered any data node yet")

// maxRecvMsgSize is the largest Plan frame (32 MiB) the client accepts, the same bound the
// liaison uses for its data-node clients.
const maxRecvMsgSize = 32 << 20

// Preflight probes every liaison candidate concurrently and keeps each one that answers, in
// the given order, after checking it is a liaison with a non-empty data-node table. A
// candidate that cannot be reached, whose answer is lost in transport, or whose registry is
// still empty is skipped with a WARN line; a reachable endpoint that refuses the credentials
// or is not a liaison is a configuration error that stops the preflight, the first such
// candidate in order winning. At least one candidate must be usable; the cluster information
// comes from the first, the primary.
func Preflight(ctx context.Context, candidates []string, opts ConnectOptions) (*Liaisons, *ClusterInfo, error) {
	if len(candidates) == 0 {
		return nil, nil, Exit(ExitUsage, "no liaison address given")
	}
	dialOpts, err := grpchelper.SecureOptions(nil, opts.EnableTLS, opts.Insecure, opts.Cert)
	if err != nil {
		return nil, nil, Exit(ExitUsage, "tls options: %w", err)
	}
	// A data node bounds each Plan frame to a few MiB, which with the liaison's additions can
	// still exceed gRPC's default 4 MiB; a large group simply spans several frames.
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize)))
	if opts.Username != "" {
		// ConnWithAuth below attaches the credentials to its health check by hand; these
		// per-RPC credentials cover every call made on the connection afterwards.
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(basicCredentials{
			username: opts.Username, password: opts.Password, secure: opts.EnableTLS,
		}))
		if !opts.EnableTLS {
			logger.GetLogger("exporter").Warn().Msg("sending credentials over a plaintext gRPC connection; enable TLS for production clusters")
		}
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	warn := opts.Log
	if warn == nil {
		warn = io.Discard
	}
	if ctx.Err() != nil {
		return nil, nil, canceled(ctx)
	}
	// Every candidate is probed at once; the results are walked in the given order so the
	// primary and the reported refusal do not depend on which probe answers first.
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]probeResult, len(candidates))
	tasks := make([]*run.Task, len(candidates))
	l := logger.GetLogger("exporter")
	for i, addr := range candidates {
		// A probe that panics leaves this error behind instead of a usable-looking result.
		results[i] = probeResult{err: fmt.Errorf("probing liaison %s failed unexpectedly", addr)}
		tasks[i] = run.Go(probeCtx, "export-preflight-probe", l, func(ctx context.Context) {
			conn, info, err := probe(ctx, addr, timeout, opts, dialOpts)
			results[i] = probeResult{conn: conn, info: info, err: err}
		})
	}
	// fail closes every connection a probe opened and returns err.
	fail := func(err error) error {
		cancel()
		for _, t := range tasks {
			t.Wait()
		}
		for _, r := range results {
			if r.conn != nil {
				_ = r.conn.Close()
			}
		}
		return err
	}
	usable := &Liaisons{}
	var primary *ClusterInfo
	var errs []error
	for i, addr := range candidates {
		<-tasks[i].Done()
		r := results[i]
		if r.err == nil {
			usable.All = append(usable.All, &Liaison{Addr: addr, Conn: r.conn})
			if primary == nil {
				primary = r.info
			}
			continue
		}
		if ctx.Err() != nil {
			return nil, nil, fail(canceled(ctx))
		}
		var skip *unusableError
		if !errors.As(r.err, &skip) {
			return nil, nil, fail(r.err)
		}
		fmt.Fprintf(warn, "WARN liaison %s is not usable, skipped: %v\n", addr, skip.err)
		errs = append(errs, skip.err)
	}
	if primary == nil {
		return nil, nil, Exit(ExitPreflight, "none of the liaison addresses is usable: %w", errors.Join(errs...))
	}
	return usable, primary, nil
}

// probeResult is the outcome of probing one candidate.
type probeResult struct {
	conn *grpc.ClientConn
	info *ClusterInfo
	err  error
}

// unusableError marks a candidate Preflight skips rather than fails on.
type unusableError struct{ err error }

func (e *unusableError) Error() string { return e.err.Error() }

func (e *unusableError) Unwrap() error { return e.err }

// probe dials one candidate and describes it. A transport-class failure or an empty registry
// is an unusableError; a refusal is a final ExitError.
func probe(ctx context.Context, addr string, timeout time.Duration, opts ConnectOptions, dialOpts []grpc.DialOption) (*grpc.ClientConn, *ClusterInfo, error) {
	// ConnWithAuth runs its own health-check deadline and takes no ctx.
	conn, err := grpchelper.ConnWithAuth(addr, timeout, opts.Username, opts.Password, dialOpts...) //nolint:contextcheck
	if err != nil {
		// The health check answered with a status that is not a transport loss: the
		// liaison is up and refused this client, so the next one would refuse it too.
		if st, isStatus := status.FromError(err); isStatus && !grpchelper.IsFailoverError(err) {
			if st.Code() == codes.Unauthenticated || st.Code() == codes.PermissionDenied {
				return nil, nil, Exit(ExitPreflight, "liaison %s refused the credentials: %w", addr, err)
			}
			return nil, nil, Exit(ExitPreflight, "liaison %s rejected the health check: %w", addr, err)
		}
		return nil, nil, &unusableError{err: fmt.Errorf("%s: %w", addr, err)}
	}
	describeCtx, cancel := context.WithTimeout(ctx, describeTimeout)
	info, err := describe(describeCtx, conn, addr)
	cancel()
	if err == nil {
		return conn, info, nil
	}
	_ = conn.Close()
	if code := status.Code(err); code == codes.Unauthenticated || code == codes.PermissionDenied {
		return nil, nil, Exit(ExitPreflight, "liaison %s refused the credentials: %w", addr, err)
	}
	if errors.Is(err, errNoDataNode) || grpchelper.IsFailoverError(err) {
		return nil, nil, &unusableError{err: err}
	}
	return nil, nil, err
}

// canceled reports a preflight the caller canceled or timed out as a runtime failure, not as
// unusable liaisons.
func canceled(ctx context.Context) error {
	return Exit(ExitRuntime, "preflight: %w", context.Cause(ctx))
}

// describe checks the endpoint is a liaison and reads its data-node table. An RPC failure
// keeps its gRPC status so Preflight can tell a transport loss from a refusal.
func describe(ctx context.Context, conn *grpc.ClientConn, addr string) (*ClusterInfo, error) {
	cur, err := databasev1.NewNodeQueryServiceClient(conn).GetCurrentNode(ctx, &databasev1.GetCurrentNodeRequest{})
	if err != nil {
		return nil, Exit(ExitPreflight, "%s: GetCurrentNode: %w", addr, err)
	}
	self := cur.GetNode()
	if !slices.Contains(self.GetRoles(), databasev1.Role_ROLE_LIAISON) {
		return nil, Exit(ExitPreflight, "%s is not a liaison (roles %v); liaison gRPC addresses are required", addr, self.GetRoles())
	}
	state, err := databasev1.NewClusterStateServiceClient(conn).GetClusterState(ctx, &databasev1.GetClusterStateRequest{})
	if err != nil {
		return nil, Exit(ExitPreflight, "%s: GetClusterState: %w", addr, err)
	}
	info := &ClusterInfo{Liaison: addr}
	if tire2 := state.GetRouteTables()["tire2"]; len(tire2.GetRegistered()) > 0 {
		info.DataNodes = tire2.GetRegistered()
		return info, nil
	}
	if slices.Contains(self.GetRoles(), databasev1.Role_ROLE_DATA) {
		info.Standalone = true
		info.DataNodes = []*databasev1.Node{self}
		return info, nil
	}
	return nil, fmt.Errorf("%s %w", addr, errNoDataNode)
}
