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
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/config"
)

// AuthInterceptor gRPC auth interceptor.
func AuthInterceptor(cfg *config.Config) func(ctxt context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "metadata is not provided")
		}
		usernameList, usernameOk := md["username"]
		passwordList, passwordOk := md["password"]
		if !usernameOk || !passwordOk {
			return nil, status.Errorf(codes.Unauthenticated, "username or password is not provided")
		}
		username := usernameList[0]
		password := passwordList[0]

		var valid bool
		for _, user := range cfg.Users {
			if username == user.Username && auth.CheckPassword(password, user.Password) {
				valid = true
				break
			}
		}
		if !valid {
			return nil, status.Errorf(codes.Unauthenticated, "invalid username or password")
		}

		return handler(ctx, req)
	}
}

// AuthStreamInterceptor gRPC auth interceptor for streams.
func AuthStreamInterceptor(cfg *config.Config) func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Errorf(codes.Unauthenticated, "metadata is not provided")
		}
		usernameList, usernameOk := md["username"]
		passwordList, passwordOk := md["password"]
		if !usernameOk || !passwordOk {
			return status.Errorf(codes.Unauthenticated, "username or password is not provided")
		}
		username := usernameList[0]
		password := passwordList[0]

		var valid bool
		for _, user := range cfg.Users {
			if username == user.Username && auth.CheckPassword(password, user.Password) {
				valid = true
				break
			}
		}
		if !valid {
			return status.Errorf(codes.Unauthenticated, "invalid username or password")
		}

		// Proceed with the stream handler if authentication is successful
		return handler(srv, ss)
	}
}
