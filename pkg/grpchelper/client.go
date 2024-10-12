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

// Package grpchelper implements helpers to access gRPC services.
package grpchelper

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Conn returns a gRPC client connection once connecting the server.
func Conn(addr string, healthCheckTimeout time.Duration, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	l := logger.GetLogger("grpc-helper")

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		l.Warn().Str("addr", addr).Err(err).Msg("error: failed to connect service")
		return nil, err
	}
	healthClient := grpc_health_v1.NewHealthClient(conn)
	deadline := time.Now().Add(healthCheckTimeout)
	for {
		if time.Now().After(deadline) {
			l.Warn().Str("addr", addr).Msg("error: health check timeout reached")
			_ = conn.Close()
			return nil, err
		}
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
		_, err = healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		cancel()
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return conn, nil
}

// Request execute a input closure to send traffics.
// It provides common features like timeout, error handling, and etc.
func Request(ctx context.Context, rpcTimeout time.Duration, fn func(rpcCtx context.Context) error) error {
	rpcStart := time.Now()
	rpcCtx, rpcCancel := context.WithTimeout(ctx, rpcTimeout)
	defer rpcCancel()
	rpcCtx = metadata.NewOutgoingContext(rpcCtx, make(metadata.MD))
	l := logger.GetLogger("grpc-helper")

	err := fn(rpcCtx)
	if err != nil {
		if stat, ok := status.FromError(err); ok && stat.Code() == codes.Unimplemented {
			l.Debug().Str("stat", stat.Message()).Msg("error: this server does not implement the service")
		} else if stat, ok := status.FromError(err); ok && stat.Code() == codes.DeadlineExceeded {
			l.Debug().Dur("rpcTimeout", rpcTimeout).Msg("timeout: rpc did not complete within")
		} else {
			l.Debug().Err(err).Msg("error: rpc failed:")
		}
		return err
	}
	rpcDuration := time.Since(rpcStart)
	if e := l.Debug(); e.Enabled() {
		e.Dur("rpc", rpcDuration).Msg("time elapsed")
	}
	return nil
}
