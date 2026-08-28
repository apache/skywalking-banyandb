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
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// NewAuthMiddleware returns the HTTP middleware that guards the grpc-gateway mux. It
// strips every header in IdentityHeaders from the incoming request, authenticates Basic
// credentials against the current security snapshot, and forwards only the verified
// identity to gRPC. Authorization remains the responsibility of the gRPC interceptor.
// Static assets and health checks with authentication disabled pass through unchanged.
func NewAuthMiddleware(authReloader *auth.Reloader) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isStaticPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			if authReloader == nil {
				next.ServeHTTP(w, r)
				return
			}
			cfg := authReloader.GetConfig()
			if cfg == nil || !cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			if r.URL.Path == "/api/healthz" && !cfg.HealthAuthEnabled {
				next.ServeHTTP(w, r)
				return
			}
			for _, header := range IdentityHeaders() {
				r.Header.Del(header)
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			if !strings.HasPrefix(authHeader, "Basic ") {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}

			encodedCredentials := strings.TrimPrefix(authHeader, "Basic ")

			decodedBytes, err := base64.StdEncoding.DecodeString(encodedCredentials)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}

			decodedCredentials := string(decodedBytes)

			parts := strings.SplitN(decodedCredentials, ":", 2)
			if len(parts) != 2 {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}

			username := parts[0]
			password := parts[1]
			if _, authenticated := authReloader.CurrentSnapshot().Authenticate(username, password); !authenticated {
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			r.Header.Set("Grpc-Metadata-Username", username)
			r.Header.Set("Grpc-Metadata-Password", password)
			next.ServeHTTP(w, r)
		})
	}
}

var staticPaths = []string{
	"/favicon.ico",
	"/banyandb.ico",
	"/assets/",
	"/index.html",
	"index.html",
}

func isStaticPath(path string) bool {
	for _, prefix := range staticPaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func buildGRPCContextForHealthCheck(authReloader *auth.Reloader, r *http.Request) (context.Context, error) {
	ctx := r.Context()
	cfg := authReloader.GetConfig()
	if cfg.HealthAuthEnabled {
		username := r.Header.Get("Grpc-Metadata-Username")
		password := r.Header.Get("Grpc-Metadata-Password")

		if username == "" || password == "" {
			return nil, errors.New("missing authentication metadata")
		}

		md := metadata.New(map[string]string{
			"username": username,
			"password": password,
		})

		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return ctx, nil
}
