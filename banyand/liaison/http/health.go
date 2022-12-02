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

package http

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func newHealthCheckClient(ctx context.Context, l *logger.Logger, addr string, opts []grpc.DialOption) (client *healthCheckClient, err error) {
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				l.Info().Str("addr", addr).Err(cerr).Msg("Failed to close conn")
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				l.Info().Str("addr", addr).Err(cerr).Msg("Failed to close conn")
			}
		}()
	}()
	return &healthCheckClient{conn: conn}, nil
}

type healthCheckClient struct {
	conn *grpc.ClientConn
}

func (g *healthCheckClient) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	var resp *grpc_health_v1.HealthCheckResponse
	if err := grpchelper.Request(ctx, 10*time.Second, func(rpcCtx context.Context) (err error) {
		resp, err = grpc_health_v1.NewHealthClient(g.conn).Check(rpcCtx,
			&grpc_health_v1.HealthCheckRequest{
				Service: "",
			})
		return err
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

func (g *healthCheckClient) Watch(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}
