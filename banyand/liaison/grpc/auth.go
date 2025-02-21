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
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

func extractUserCredentialsFromContext(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Errorf(codes.Unauthenticated, "metadata is not provided")
	}
	usernameList, usernameOk := md["username"]
	passwordList, passwordOk := md["password"]
	if !usernameOk || len(usernameList) == 0 {
		return "", "", status.Errorf(codes.Unauthenticated, "username is not provided correctly")
	}
	if !passwordOk || len(passwordList) == 0 {
		return "", "", status.Errorf(codes.Unauthenticated, "password is not provided correctly")
	}
	username := usernameList[0]
	password := passwordList[0]
	return username, password, nil
}

func checkUsernameAndPassword(username, password string) bool {
	for _, user := range auth.Cfg.Users {
		if strings.TrimSpace(username) == strings.TrimSpace(user.Username) &&
			strings.TrimSpace(password) == strings.TrimSpace(user.Password) {
			return true
		}
	}
	return false
}

// authInterceptor gRPC auth interceptor.
func authInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !auth.Cfg.Enabled {
		return handler(ctx, req)
	}

	username, password, err := extractUserCredentialsFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if checkUsernameAndPassword(username, password) {
		return handler(ctx, req)
	}
	return nil, status.Errorf(codes.Unauthenticated, "invalid username or password")
}

// authStreamInterceptor gRPC auth interceptor for streams.
func authStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !auth.Cfg.Enabled {
		return handler(srv, ss)
	}

	username, password, err := extractUserCredentialsFromContext(ss.Context())
	if err != nil {
		return err
	}

	if checkUsernameAndPassword(username, password) {
		return handler(srv, ss)
	}
	return status.Errorf(codes.Unauthenticated, "invalid username or password")
}
