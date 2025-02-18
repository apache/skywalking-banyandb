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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/config"
)

var cfg = &config.Config{
	Enabled: true,
	Users: []config.User{
		{
			Username: "test",
			Password: "$2a$10$Dty9D1PMVx0kt24S09qs6ezn2Q77wLsnmlpU6iO29hMn.Urbo.uji",
		},
	},
}

// Mock handler to simulate GRPC behavior.
func mockHandler(_ context.Context, _ any) (any, error) {
	return "success", nil
}

func TestAuthInterceptor(t *testing.T) {
	// Create the interceptor.
	interceptor := mockAuthInterceptor

	tests := []struct {
		name            string
		md              metadata.MD
		expectedError   error
		expectedMessage string
	}{
		{
			name: "Valid credentials",
			md: metadata.MD{
				"username": []string{"test"},
				"password": []string{"password"},
			},
			expectedError:   nil,
			expectedMessage: "success",
		},
		{
			name: "Invalid username",
			md: metadata.MD{
				"username": []string{"wronguser"},
				"password": []string{"password"},
			},
			expectedError:   status.Errorf(codes.Unauthenticated, "invalid username or password"),
			expectedMessage: "",
		},
		{
			name: "Invalid password",
			md: metadata.MD{
				"username": []string{"test"},
				"password": []string{"wrongpassword"},
			},
			expectedError:   status.Errorf(codes.Unauthenticated, "invalid username or password"),
			expectedMessage: "",
		},
		{
			name:            "Missing username",
			md:              metadata.MD{},
			expectedError:   status.Errorf(codes.Unauthenticated, "username is not provided correctly"),
			expectedMessage: "",
		},
		{
			name: "Missing password",
			md: metadata.MD{
				"username": []string{"test"},
			},
			expectedError:   status.Errorf(codes.Unauthenticated, "password is not provided correctly"),
			expectedMessage: "",
		},
	}

	// Iterate over test cases.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create context with metadata.
			ctx := metadata.NewIncomingContext(context.Background(), tt.md)

			// Call the interceptor.
			resp, err := interceptor(ctx, nil, nil, mockHandler)

			// Assert the response and error.
			if tt.expectedError != nil {
				assert.ErrorIs(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMessage, resp)
			}
		})
	}
}

func mockAuthInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !cfg.Enabled {
		return handler(ctx, req)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "metadata is not provided")
	}
	usernameList, usernameOk := md["username"]
	passwordList, passwordOk := md["password"]
	if !usernameOk || len(usernameList) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "username is not provided correctly")
	}
	if !passwordOk || len(passwordList) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "password is not provided correctly")
	}
	username := usernameList[0]
	password := passwordList[0]

	for _, user := range cfg.Users {
		if strings.ReplaceAll(username, " ", "") == strings.ReplaceAll(user.Username, " ", "") &&
			auth.CheckPassword(strings.ReplaceAll(password, " ", ""), strings.ReplaceAll(user.Password, " ", "")) {
			return handler(ctx, req)
		}
	}
	return nil, status.Errorf(codes.Unauthenticated, "invalid username or password")
}
