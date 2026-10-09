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
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// ClusterInfo is what the connection preflight learned about the source cluster.
type ClusterInfo struct {
	Liaison    string
	DataNodes  []*databasev1.Node
	Standalone bool
}

// ConnectOptions are the dial parameters shared by every liaison candidate.
type ConnectOptions struct {
	Cert      string
	Username  string
	Password  string
	Timeout   time.Duration
	EnableTLS bool
	Insecure  bool
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

// describeTimeout bounds one candidate's describe RPCs so a hung liaison does not keep the
// next candidate from being tried.
var describeTimeout = 10 * time.Second

// errNoDataNode marks a liaison that answered but has not discovered any data node yet.
var errNoDataNode = errors.New("has not discovered any data node yet")

// maxRecvMsgSize is the largest Plan frame (32 MiB) the client accepts, the same bound the
// liaison uses for its data-node clients.
const maxRecvMsgSize = 32 << 20

// Preflight tries the liaison candidates in order and returns the first one that answers,
// after checking it is a liaison and reading the data-node table (design §2.3 step 1).
// A candidate that cannot be reached, whose answer is lost in transport, or whose registry
// is still empty is skipped; a reachable endpoint that refuses the credentials or is not a
// liaison is a configuration error, not a retry.
func Preflight(ctx context.Context, candidates []string, opts ConnectOptions) (*grpc.ClientConn, *ClusterInfo, error) {
	if len(candidates) == 0 {
		return nil, nil, Exit(ExitUsage, "no liaison address given")
	}
	dialOpts, err := grpchelper.SecureOptions(nil, opts.EnableTLS, opts.Insecure, opts.Cert)
	if err != nil {
		return nil, nil, Exit(ExitUsage, "tls options: %w", err)
	}
	// One Plan frame carries every unit of one group, which can exceed gRPC's default 4 MiB.
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
	var errs []error
	for _, addr := range candidates {
		if ctx.Err() != nil {
			return nil, nil, canceled(ctx)
		}
		// ConnWithAuth runs its own health-check deadline and takes no ctx.
		conn, dialErr := grpchelper.ConnWithAuth(addr, timeout, opts.Username, opts.Password, dialOpts...) //nolint:contextcheck
		if dialErr != nil {
			// The health check answered with a status that is not a transport loss: the
			// liaison is up and refused this client, so the next one would refuse it too.
			if st, isStatus := status.FromError(dialErr); isStatus && !grpchelper.IsFailoverError(dialErr) {
				if st.Code() == codes.Unauthenticated || st.Code() == codes.PermissionDenied {
					return nil, nil, Exit(ExitPreflight, "liaison %s refused the credentials: %w", addr, dialErr)
				}
				return nil, nil, Exit(ExitPreflight, "liaison %s rejected the health check: %w", addr, dialErr)
			}
			errs = append(errs, fmt.Errorf("%s: %w", addr, dialErr))
			continue
		}
		describeCtx, cancel := context.WithTimeout(ctx, describeTimeout)
		info, infoErr := describe(describeCtx, conn, addr)
		cancel()
		if infoErr == nil {
			return conn, info, nil
		}
		_ = conn.Close()
		if ctx.Err() != nil {
			return nil, nil, canceled(ctx)
		}
		if code := status.Code(infoErr); code == codes.Unauthenticated || code == codes.PermissionDenied {
			return nil, nil, Exit(ExitPreflight, "liaison %s refused the credentials: %w", addr, infoErr)
		}
		if !errors.Is(infoErr, errNoDataNode) && !grpchelper.IsFailoverError(infoErr) {
			return nil, nil, infoErr
		}
		errs = append(errs, infoErr)
	}
	return nil, nil, Exit(ExitPreflight, "none of the liaison addresses is usable: %w", errors.Join(errs...))
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
