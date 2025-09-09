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

package grpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

func authInterceptor(authReloader *auth.Reloader) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		cfg := authReloader.GetConfig()
		if !cfg.Enabled {
			return handler(ctx, req)
		}
		if info.FullMethod == "/grpc.health.v1.Health/Check" && !cfg.HealthAuthEnabled {
			return handler(ctx, req)
		}
		if err := validateUser(ctx, authReloader); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func authStreamInterceptor(authReloader *auth.Reloader) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		cfg := authReloader.GetConfig()
		if !cfg.Enabled {
			return handler(srv, stream)
		}
		if info.FullMethod == "/grpc.health.v1.Health/Check" && !cfg.HealthAuthEnabled {
			return handler(srv, stream)
		}
		if err := validateUser(stream.Context(), authReloader); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func validateUser(ctx context.Context, authReloader *auth.Reloader) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "metadata is not provided")
	}

	usernames := md.Get("username")
	passwords := md.Get("password")

	if len(usernames) == 0 || len(passwords) == 0 {
		return status.Errorf(codes.Unauthenticated, "Invalid credentials")
	}

	username := usernames[0]
	password := passwords[0]

	if !authReloader.CheckUsernameAndPassword(username, password) {
		return status.Errorf(codes.Unauthenticated, "Invalid credentials")
	}
	return nil
}
