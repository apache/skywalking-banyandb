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
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

func TestAuthMiddleware(t *testing.T) {
	testUser := auth.User{Username: "test", Password: "password"}

	tests := []struct {
		wantHeaders map[string]string
		name        string
		authHeader  string
		wantBody    string
		users       []auth.User
		wantStatus  int
		enableAuth  bool
	}{
		{
			name:        "auth disabled no header",
			enableAuth:  false,
			users:       nil,
			authHeader:  "",
			wantStatus:  http.StatusOK,
			wantBody:    "",
			wantHeaders: nil,
		},
		{
			name:       "auth enabled no header",
			enableAuth: true,
			users:      []auth.User{testUser},
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
			wantBody:   "Authorization header is missing",
			wantHeaders: map[string]string{
				"WWW-Authenticate": `Basic realm="Restricted"`,
			},
		},
		{
			name:        "invalid auth scheme",
			enableAuth:  true,
			users:       []auth.User{testUser},
			authHeader:  "Bearer token",
			wantStatus:  http.StatusBadRequest,
			wantBody:    "Invalid authorization header format",
			wantHeaders: nil,
		},
		{
			name:        "invalid base64 encoding",
			enableAuth:  true,
			users:       []auth.User{testUser},
			authHeader:  "Basic invalid-base64",
			wantStatus:  http.StatusBadRequest,
			wantBody:    "Failed to decode authorization header",
			wantHeaders: nil,
		},
		{
			name:        "malformed credentials",
			enableAuth:  true,
			users:       []auth.User{testUser},
			authHeader:  "Basic " + base64.StdEncoding.EncodeToString([]byte("test")),
			wantStatus:  http.StatusBadRequest,
			wantBody:    "Invalid authorization header format",
			wantHeaders: nil,
		},
		{
			name:        "valid credentials",
			enableAuth:  true,
			users:       []auth.User{testUser},
			authHeader:  "Basic " + base64.StdEncoding.EncodeToString([]byte("test:password")),
			wantStatus:  http.StatusOK,
			wantBody:    "",
			wantHeaders: nil,
		},
		{
			name:        "wrong password",
			enableAuth:  true,
			users:       []auth.User{testUser},
			authHeader:  "Basic " + base64.StdEncoding.EncodeToString([]byte("test:wrong")),
			wantStatus:  http.StatusUnauthorized,
			wantBody:    `{"error": "invalid credentials"}`,
			wantHeaders: nil,
		},
		{
			name:        "unknown user",
			enableAuth:  true,
			users:       []auth.User{testUser},
			authHeader:  "Basic " + base64.StdEncoding.EncodeToString([]byte("unknown:password")),
			wantStatus:  http.StatusUnauthorized,
			wantBody:    `{"error": "invalid credentials"}`,
			wantHeaders: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCfg := &auth.Config{
				Users:   tt.users,
				Enabled: tt.enableAuth,
			}
			auth.Cfg = testCfg

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rr := httptest.NewRecorder()

			handler := authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			handler.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("unexpected status code: got %d want %d", rr.Code, tt.wantStatus)
			}

			if strings.TrimSpace(rr.Body.String()) != tt.wantBody {
				t.Errorf("unexpected response body: got %q want %q", rr.Body.String(), tt.wantBody)
			}

			for hdr, want := range tt.wantHeaders {
				if got := rr.Header().Get(hdr); got != want {
					t.Errorf("unexpected header %s: got %q want %q", hdr, got, want)
				}
			}
		})
	}
}
