// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package helpers

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var errServiceUnhealthy = errors.New("service is unhealthy")

// HealthCheck returns a function for ginkgo "Eventually" poll it repeatedly to check whether a gRPC server is ready.
func HealthCheck(addr string, connTimeout time.Duration, rpcTimeout time.Duration, opts ...grpc.DialOption) func() error {
	return HealthCheckWithAuth(addr, connTimeout, rpcTimeout, "", "", opts...)
}

// HealthCheckWithAuth returns a function for ginkgo "Eventually" poll it repeatedly to check whether a gRPC server is ready with Auth.
func HealthCheckWithAuth(addr string, connTimeout time.Duration, rpcTimeout time.Duration, username, password string, opts ...grpc.DialOption) func() error {
	return func() error {
		conn, err := grpchelper.ConnWithAuth(addr, connTimeout, username, password, opts...)
		if err != nil {
			return err
		}
		defer conn.Close()
		var resp *grpc_health_v1.HealthCheckResponse
		if err := grpchelper.Request(context.Background(), rpcTimeout, func(rpcCtx context.Context) (err error) {
			md := metadata.Pairs(
				"username", username,
				"password", password,
			)
			rpcCtx = metadata.NewOutgoingContext(rpcCtx, md)
			resp, err = grpc_health_v1.NewHealthClient(conn).Check(rpcCtx,
				&grpc_health_v1.HealthCheckRequest{
					Service: "",
				})
			return err
		}); err != nil {
			return err
		}
		l := logger.GetLogger()
		if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			l.Warn().Str("responded_status", resp.GetStatus().String()).Msg("service unhealthy")
			return errServiceUnhealthy
		}
		if e := l.Debug(); e.Enabled() {
			e.Stringer("status", resp.GetStatus()).Msg("connected")
		}
		return nil
	}
}
