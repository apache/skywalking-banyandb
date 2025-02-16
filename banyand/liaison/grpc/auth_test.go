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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/config"
)

// Mock handler to simulate GRPC behavior.
func mockHandler(_ context.Context, _ any) (any, error) {
	return "success", nil
}

func TestAuthInterceptor(t *testing.T) {
	// Create a mock configuration.
	cfg := &config.Config{
		Enabled: true,
		Users: []config.User{
			{
				Username: "test",
				Password: "$2a$10$Dty9D1PMVx0kt24S09qs6ezn2Q77wLsnmlpU6iO29hMn.Urbo.uji",
			},
		},
	}

	// Create the interceptor.
	interceptor := AuthInterceptor(cfg)

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
			name:            "Missing credentials",
			md:              metadata.MD{},
			expectedError:   status.Errorf(codes.Unauthenticated, "username or password is not provided"),
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
