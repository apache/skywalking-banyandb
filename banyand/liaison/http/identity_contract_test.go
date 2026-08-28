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

package http_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	liaisonhttp "github.com/apache/skywalking-banyandb/banyand/liaison/http"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	pkgauth "github.com/apache/skywalking-banyandb/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const httpPolicyYAML = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-reader"
    password: "reader-secret"
rbac:
  enabled: true
  roles: {}
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-reader"
      role: "reader"
      groups: ["sw_metric"]
`

const httpUsersOnlyYAML = `
users:
  - username: "alice"
    password: "secret"
`

func reloaderFor(t *testing.T, raw string) *auth.Reloader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	reloader := auth.InitAuthReloader()
	if err := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-http-contract-test")); err != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want it to accept the fixture", path, err)
	}
	return reloader
}

// forwarded records the request the middleware handed to the gateway, which is the only
// thing downstream can act on.
type forwarded struct {
	req    *http.Request
	called bool
}

func (f *forwarded) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	f.called = true
	f.req = r
}

// TestR4_ForgedGatewayIdentityCannotReplaceBasicAuth proves R4's HTTP half. The
// grpc-gateway turns `Grpc-Metadata-*` headers into gRPC metadata, so a caller that sets
// them itself is attempting to hand the authorization boundary an identity nobody
// verified. Presenting reader credentials while claiming to be the admin must forward the
// reader.
//
// The middleware must strip these headers on the way in, not merely overwrite the two it
// happens to set, because any identity header it leaves standing reaches the interceptor.
func TestR4_ForgedGatewayIdentityCannotReplaceBasicAuth(t *testing.T) {
	names := liaisonhttp.IdentityHeaders()
	if len(names) == 0 {
		t.Fatal("IdentityHeaders() returned nothing, want the headers that carry identity across the gateway")
	}
	for _, name := range names {
		if !strings.HasPrefix(http.CanonicalHeaderKey(name), "Grpc-Metadata-") {
			t.Errorf("IdentityHeaders() contains %q, want only headers the gateway forwards as metadata", name)
		}
	}

	next := &forwarded{}
	handler := liaisonhttp.NewAuthMiddleware(reloaderFor(t, httpPolicyYAML))(next)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshot", nil)
	req.Header.Set("Authorization", pkgauth.GenerateBasicAuthHeader("bydb-reader", "reader-secret"))
	for _, name := range names {
		req.Header.Set(name, "bydb-admin")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated request returned %d, want %d — the middleware authenticates and forwards, it does not authorize",
			rec.Code, http.StatusOK)
	}
	if !next.called {
		t.Fatal("the middleware did not forward an authenticated request to the gateway")
	}
	for _, name := range names {
		if got := next.req.Header.Get(name); got == "bydb-admin" {
			t.Errorf("header %q reached the gateway as %q, want the forged value replaced by the authenticated identity", name, got)
		}
	}
	if got := next.req.Header.Get("Grpc-Metadata-Username"); got != "bydb-reader" {
		t.Errorf("the gateway received username %q, want the authenticated %q", got, "bydb-reader")
	}
}

// TestR4_UnauthenticatedRequestsAreRejectedBeforeTheGateway proves the middleware never
// forwards a request whose credentials it could not verify, so the gateway is never asked
// to carry an unauthenticated identity. The status codes are the ones a client observes.
func TestR4_UnauthenticatedRequestsAreRejectedBeforeTheGateway(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
		want   int
	}{
		{name: "no Authorization header", header: "", want: http.StatusUnauthorized},
		{name: "wrong password", header: pkgauth.GenerateBasicAuthHeader("bydb-reader", "wrong"), want: http.StatusUnauthorized},
		{name: "unknown user", header: pkgauth.GenerateBasicAuthHeader("nobody", "reader-secret"), want: http.StatusUnauthorized},
		{name: "not Basic", header: "Bearer some-token", want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := &forwarded{}
			handler := liaisonhttp.NewAuthMiddleware(reloaderFor(t, httpPolicyYAML))(next)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/state", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			// A forged identity must not rescue a request that fails authentication.
			req.Header.Set("Grpc-Metadata-Username", "bydb-admin")
			req.Header.Set("Grpc-Metadata-Password", "admin-secret")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("%s returned %d, want %d", tc.name, rec.Code, tc.want)
			}
			if next.called {
				t.Errorf("%s was forwarded to the gateway, want it rejected at the middleware", tc.name)
			}
		})
	}
}

// TestR4_RejectionBodiesCarryNoCredentials proves the no-leak half of R4: nothing the
// middleware writes back, and no header it sets on a rejection, may repeat a credential
// the caller sent or one the snapshot holds.
func TestR4_RejectionBodiesCarryNoCredentials(t *testing.T) {
	next := &forwarded{}
	handler := liaisonhttp.NewAuthMiddleware(reloaderFor(t, httpPolicyYAML))(next)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/state", nil)
	req.Header.Set("Authorization", pkgauth.GenerateBasicAuthHeader("bydb-reader", "wrong-but-secret"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong credentials returned %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	haystack := rec.Body.String()
	for name, values := range rec.Result().Header {
		haystack += " " + name + ": " + strings.Join(values, ",")
	}
	for _, secret := range []string{"wrong-but-secret", "admin-secret", "reader-secret", req.Header.Get("Authorization")} {
		if strings.Contains(haystack, secret) {
			t.Errorf("the rejection response repeats the credential %q", secret)
		}
	}
}

// TestR2_UsersOnlyDeploymentIsUnchangedAtTheHTTPSeam proves R2 at the HTTP boundary: a
// deployment whose security file predates this milestone keeps authenticating its users
// and keeps forwarding them, with no new rejection introduced by the RBAC machinery.
func TestR2_UsersOnlyDeploymentIsUnchangedAtTheHTTPSeam(t *testing.T) {
	next := &forwarded{}
	handler := liaisonhttp.NewAuthMiddleware(reloaderFor(t, httpUsersOnlyYAML))(next)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/group/schema/lists", nil)
	req.Header.Set("Authorization", pkgauth.GenerateBasicAuthHeader("alice", "secret"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("alice on a users-only deployment returned %d, want %d", rec.Code, http.StatusOK)
	}
	if !next.called {
		t.Fatal("alice was not forwarded to the gateway on a users-only deployment")
	}
	if got := next.req.Header.Get("Grpc-Metadata-Username"); got != "alice" {
		t.Errorf("the gateway received username %q, want %q", got, "alice")
	}
}

// TestR2_StaticAssetsBypassAuthentication proves the last compatibility carve-out the
// existing middleware has: the UI's static assets are served without credentials, so
// turning RBAC on does not break the console's first paint.
func TestR2_StaticAssetsBypassAuthentication(t *testing.T) {
	for _, path := range []string{"/favicon.ico", "/banyandb.ico", "/assets/index.js"} {
		next := &forwarded{}
		handler := liaisonhttp.NewAuthMiddleware(reloaderFor(t, httpPolicyYAML))(next)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if !next.called {
			t.Errorf("static path %s was not forwarded, want it served without credentials", path)
		}
		if rec.Code != http.StatusOK {
			t.Errorf("static path %s returned %d, want %d", path, rec.Code, http.StatusOK)
		}
	}
}
