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

package common

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
)

// GRPCDialOptionsProvider provides gRPC dial options for TLS configuration.
type GRPCDialOptionsProvider interface {
	GetDialOptions(address string) ([]grpc.DialOption, error)
}

// FetchNodeMetadata fetches node metadata via gRPC.
// This is the common implementation used by both DNS and file discovery.
func FetchNodeMetadata(ctx context.Context, address string, timeout time.Duration, dialOptsProvider GRPCDialOptionsProvider) (*databasev1.Node, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialOpts, err := dialOptsProvider.GetDialOptions(address)
	if err != nil {
		return nil, fmt.Errorf("failed to get dial options for %s: %w", address, err)
	}

	// nolint:contextcheck
	conn, connErr := grpchelper.Conn(address, timeout, dialOpts...)
	if connErr != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, connErr)
	}
	defer conn.Close()

	client := databasev1.NewNodeQueryServiceClient(conn)
	resp, callErr := client.GetCurrentNode(ctxTimeout, &databasev1.GetCurrentNodeRequest{})
	if callErr != nil {
		return nil, fmt.Errorf("failed to get current node from %s: %w", address, callErr)
	}

	return resp.GetNode(), nil
}
