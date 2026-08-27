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
	"net/http"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// IdentityHeaders returns the request headers that carry a caller identity across the
// grpc-gateway boundary. The gateway turns each of them into gRPC metadata, so a request
// arriving from the network must never be allowed to set one itself.
func IdentityHeaders() []string {
	return nil
}

// NewAuthMiddleware returns the HTTP middleware that guards the grpc-gateway mux. It
// strips every header in IdentityHeaders from the incoming request, performs Basic
// authentication against the security snapshot in force, and re-sets those headers from
// the credentials it verified, so the identity the gateway forwards is always the
// authenticated one.
//
// The middleware authenticates only. The authoritative authorization decision belongs to
// the gRPC unary interceptor behind the gateway, which is what makes one HTTP request
// produce exactly one decision.
//
// Static asset paths and, when health-check authentication is disabled, the health
// endpoint pass through untouched, exactly as they did before this middleware existed.
func NewAuthMiddleware(_ *auth.Reloader) func(http.Handler) http.Handler {
	return func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "the security middleware is not built yet", http.StatusNotImplemented)
		})
	}
}
