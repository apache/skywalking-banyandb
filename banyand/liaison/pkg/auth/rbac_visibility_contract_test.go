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

package auth_test

import (
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// visibilityPolicyYAML binds one exact-scope reader, one wildcard reader, one exact-scope
// writer and one principal with no binding, which is the smallest family that separates
// "holds the permission somewhere" from "holds the permission for this group" and from
// "holds the permission for every group".
const visibilityPolicyYAML = `
users:
  - username: "vis-reader-alpha"
    password: "reader-alpha-secret"
  - username: "vis-reader-all"
    password: "reader-all-secret"
  - username: "vis-writer-alpha"
    password: "writer-alpha-secret"
  - username: "vis-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  bindings:
    - principal: "vis-reader-alpha"
      role: "reader"
      groups: ["rbac-alpha"]
    - principal: "vis-reader-all"
      role: "reader"
      groups: ["*"]
    - principal: "vis-writer-alpha"
      role: "writer"
      groups: ["rbac-alpha"]
`

const visibilityUsersOnlyYAML = `
users:
  - username: "vis-reader-alpha"
    password: "reader-alpha-secret"
`

// TestSchemaR3_AllowsAnyReportsHoldingAPermissionSomewhere proves the visibility half of R3 at the
// snapshot seam. A method whose resource set is the whole deployment cannot ask "does this
// principal hold the permission for group X" before its handler has produced the groups, so
// the snapshot must answer the weaker question first: does the principal hold it anywhere.
// Every want below is read off the bindings in the fixture above.
func TestSchemaR3_AllowsAnyReportsHoldingAPermissionSomewhere(t *testing.T) {
	snapshot := compile(t, 1, visibilityPolicyYAML)

	for _, tc := range []struct {
		username string
		password string
		perm     auth.Permission
		want     bool
	}{
		// An exact-scope grant is a grant: the reader holds schema:read, on one group.
		{username: "vis-reader-alpha", password: "reader-alpha-secret", perm: auth.PermissionSchemaRead, want: true},
		// It is still not a wildcard grant, which Allows with no group asks for.
		{username: "vis-reader-alpha", password: "reader-alpha-secret", perm: auth.PermissionSchemaWrite, want: false},
		{username: "vis-reader-alpha", password: "reader-alpha-secret", perm: auth.PermissionClusterRead, want: false},
		{username: "vis-reader-all", password: "reader-all-secret", perm: auth.PermissionSchemaRead, want: true},
		{username: "vis-reader-all", password: "reader-all-secret", perm: auth.PermissionSchemaWrite, want: false},
		{username: "vis-writer-alpha", password: "writer-alpha-secret", perm: auth.PermissionSchemaRead, want: true},
		{username: "vis-writer-alpha", password: "writer-alpha-secret", perm: auth.PermissionSchemaWrite, want: true},
		{username: "vis-writer-alpha", password: "writer-alpha-secret", perm: auth.PermissionClusterAdmin, want: false},
		// An authenticated principal with no binding holds nothing anywhere.
		{username: "vis-unbound", password: "unbound-secret", perm: auth.PermissionSchemaRead, want: false},
		{username: "vis-unbound", password: "unbound-secret", perm: auth.PermissionDataRead, want: false},
	} {
		user := principal(t, snapshot, tc.username, tc.password)
		if got := snapshot.AllowsAny(user, tc.perm); got != tc.want {
			t.Errorf("AllowsAny(%s, %q) = %v, want %v", tc.username, tc.perm, got, tc.want)
		}
	}

	t.Run("the zero principal holds nothing", func(t *testing.T) {
		for _, perm := range auth.Permissions() {
			if snapshot.AllowsAny(auth.Principal{}, perm) {
				t.Errorf("AllowsAny(zero principal, %q) = true, want false", perm)
			}
		}
	})

	t.Run("an exact grant that Allows one group is reported by both", func(t *testing.T) {
		reader := principal(t, snapshot, "vis-reader-alpha", "reader-alpha-secret")
		if !snapshot.Allows(reader, auth.PermissionSchemaRead, "rbac-alpha") {
			t.Error("Allows(vis-reader-alpha, schema:read, rbac-alpha) = false, want true for its bound group")
		}
		if snapshot.Allows(reader, auth.PermissionSchemaRead, "rbac-beta") {
			t.Error("Allows(vis-reader-alpha, schema:read, rbac-beta) = true, want false outside its bound group")
		}
		if !snapshot.AllowsAny(reader, auth.PermissionSchemaRead) {
			t.Error("AllowsAny(vis-reader-alpha, schema:read) = false, want true because one group is bound")
		}
	})

	t.Run("a users-only policy grants nothing anywhere", func(t *testing.T) {
		usersOnly := compile(t, 2, visibilityUsersOnlyYAML)
		reader := principal(t, usersOnly, "vis-reader-alpha", "reader-alpha-secret")
		for _, perm := range auth.Permissions() {
			if usersOnly.AllowsAny(reader, perm) {
				t.Errorf("AllowsAny(%q) = true with RBAC disabled, want false", perm)
			}
		}
	})
}
