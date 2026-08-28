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
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// fixedPolicyYAML is the one owner-only users+RBAC configuration family the whole
// milestone is oracled against. The five actors and their answers come from issue
// #14014 ("fixed admin/monitor/reader/writer/unbound answers"); `monitor` is the flat
// custom role round A4 requires, and `unbound` is a user with credentials and no binding.
const fixedPolicyYAML = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-monitor"
    password: "monitor-secret"
  - username: "bydb-reader"
    password: "reader-secret"
  - username: "bydb-writer"
    password: "writer-secret"
  - username: "bydb-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: ["cluster:read"]
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-monitor"
      role: "monitor"
      groups: ["*"]
    - principal: "bydb-reader"
      role: "reader"
      groups: ["sw_metric"]
    - principal: "bydb-writer"
      role: "writer"
      groups: ["*"]
`

// usersOnlyYAML is a configuration file written before this milestone existed. It is
// copied verbatim from the shape banyand/liaison/pkg/auth/reloader_test.go already loads.
const usersOnlyYAML = `
users:
  - username: "alice"
    password: "secret"
  - username: "bob"
    password: "hunter2"
`

// truncatedYAML is what a reader observes if it wakes on the fsnotify event after the
// writer opened the file and before it finished writing: a document that stops mid-token.
const truncatedYAML = "users:\n  - username: \"a\"\n    password: \"b\"\n" +
	"rbac:\n  enabled: true\n  roles:\n    - name: \"r\"\n      permissions: [\"cluster"

// rbacDisabledYAML sets rbac.enabled explicitly to false and still declares roles and
// bindings, to prove the flag alone decides and a present block is not an implicit opt-in.
const rbacDisabledYAML = `
users:
  - username: "alice"
    password: "secret"
rbac:
  enabled: false
  roles: {}
  bindings:
    - principal: "alice"
      role: "admin"
      groups: ["*"]
`

func writeSecurityFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

func compile(t *testing.T, revision uint64, raw string) auth.Snapshot {
	t.Helper()
	snap, err := auth.CompileSnapshot(revision, []byte(raw))
	if err != nil {
		t.Fatalf("CompileSnapshot(%d, fixture) returned error %v, want a compiled snapshot", revision, err)
	}
	if snap == nil {
		t.Fatalf("CompileSnapshot(%d, fixture) returned a nil snapshot, want a compiled snapshot", revision)
	}
	return snap
}

func principal(t *testing.T, snap auth.Snapshot, username, password string) auth.Principal {
	t.Helper()
	p, ok := snap.Authenticate(username, password)
	if !ok {
		t.Fatalf("Authenticate(%q, %q) = _, false; want the fixture credentials to verify", username, password)
	}
	return p
}

// TestR2_UsersOnlyFileStaysCompatible proves R2: a security file written before this
// milestone — one carrying only a `users` list — still compiles, still authenticates its
// users, and leaves RBAC off. An explicit `rbac.enabled: false` behaves identically even
// when roles and bindings are present, so a present block is never an implicit opt-in.
//
// This is the "old bytes still read" gate for the one durable format this milestone
// touches: the operator-authored security YAML.
func TestR2_UsersOnlyFileStaysCompatible(t *testing.T) {
	for _, tc := range []struct {
		name     string
		raw      string
		username string
		password string
	}{
		{name: "users-only", raw: usersOnlyYAML, username: "alice", password: "secret"},
		{name: "rbac-explicitly-disabled", raw: rbacDisabledYAML, username: "alice", password: "secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := compile(t, 1, tc.raw)
			if snap.RBACEnabled() {
				t.Errorf("RBACEnabled() = true, want false for a %s file", tc.name)
			}
			if _, ok := snap.Authenticate(tc.username, tc.password); !ok {
				t.Errorf("Authenticate(%q, %q) = _, false; want the pre-existing credentials to keep working", tc.username, tc.password)
			}
			if _, ok := snap.Authenticate(tc.username, tc.password+"-wrong"); ok {
				t.Errorf("Authenticate(%q, wrong password) = _, true; want false", tc.username)
			}
			if _, ok := snap.Authenticate("nobody", "secret"); ok {
				t.Error(`Authenticate("nobody", "secret") = _, true; want false`)
			}
		})
	}
}

// TestR2_DisabledSnapshotGrantsNothing proves the other half of R2: when RBAC is off the
// snapshot answers no permission question affirmatively, so an authenticated caller under
// a users-only file is never routed through a grant lookup that could invent a permission.
// Authorization is the caller's decision to skip, not the snapshot's to fake.
func TestR2_DisabledSnapshotGrantsNothing(t *testing.T) {
	snap := compile(t, 1, rbacDisabledYAML)
	alice := principal(t, snap, "alice", "secret")
	for _, perm := range auth.Permissions() {
		if snap.Allows(alice, perm) {
			t.Errorf("Allows(alice, %q) = true with RBAC disabled, want false", perm)
		}
	}
}

// TestR2_FixedRoleMatrix proves the compiled grants of the fixed policy family. Every
// expected cell is read off the fixture above, which in turn encodes issue #14014's fixed
// admin/monitor/reader/writer/unbound answers; nothing here recomputes a grant the way the
// compiler will. `bydb-unbound` has valid credentials and no binding, so it holds nothing.
func TestR2_FixedRoleMatrix(t *testing.T) {
	snap := compile(t, 7, fixedPolicyYAML)
	if got := snap.Revision(); got != 7 {
		t.Errorf("Revision() = %d, want the 7 it was compiled at", got)
	}
	if !snap.RBACEnabled() {
		t.Fatal("RBACEnabled() = false, want true for the fixed enabled policy")
	}

	want := map[string]map[auth.Permission]bool{
		"bydb-admin": {
			auth.PermissionClusterRead: true, auth.PermissionClusterAdmin: true,
			auth.PermissionSchemaRead: true, auth.PermissionSchemaWrite: true,
			auth.PermissionDataRead: true, auth.PermissionDataWrite: true,
		},
		"bydb-monitor": {
			auth.PermissionClusterRead: true, auth.PermissionClusterAdmin: false,
			auth.PermissionSchemaRead: false, auth.PermissionSchemaWrite: false,
			auth.PermissionDataRead: false, auth.PermissionDataWrite: false,
		},
		"bydb-reader": {
			auth.PermissionClusterRead: false, auth.PermissionClusterAdmin: false,
			auth.PermissionSchemaRead: true, auth.PermissionSchemaWrite: false,
			auth.PermissionDataRead: true, auth.PermissionDataWrite: false,
		},
		"bydb-writer": {
			auth.PermissionClusterRead: false, auth.PermissionClusterAdmin: false,
			auth.PermissionSchemaRead: true, auth.PermissionSchemaWrite: true,
			auth.PermissionDataRead: true, auth.PermissionDataWrite: true,
		},
		"bydb-unbound": {
			auth.PermissionClusterRead: false, auth.PermissionClusterAdmin: false,
			auth.PermissionSchemaRead: false, auth.PermissionSchemaWrite: false,
			auth.PermissionDataRead: false, auth.PermissionDataWrite: false,
		},
	}
	passwords := map[string]string{
		"bydb-admin": "admin-secret", "bydb-monitor": "monitor-secret",
		"bydb-reader": "reader-secret", "bydb-writer": "writer-secret",
		"bydb-unbound": "unbound-secret",
	}
	for username, row := range want {
		user := principal(t, snap, username, passwords[username])
		if user.Username() != username {
			t.Errorf("Principal.Username() = %q, want %q", user.Username(), username)
		}
		scope := "*"
		if username == "bydb-reader" {
			scope = "sw_metric"
		}
		for perm, allowed := range row {
			if got := snap.Allows(user, perm, scope); got != allowed {
				t.Errorf("Allows(%s, %s) = %v, want %v", username, perm, got, allowed)
			}
		}
	}
}

// TestR2_ZeroPrincipalHoldsNothing proves that the zero Principal — the value any caller
// can construct without presenting credentials — is never granted anything. This is the
// value an interceptor would hold if it forgot to authenticate first.
func TestR2_ZeroPrincipalHoldsNothing(t *testing.T) {
	snap := compile(t, 1, fixedPolicyYAML)
	var anonymous auth.Principal
	if !anonymous.IsZero() {
		t.Error("the zero Principal reports IsZero() = false, want true")
	}
	if anonymous.Username() != "" {
		t.Errorf("the zero Principal reports Username() = %q, want the empty string", anonymous.Username())
	}
	for _, perm := range auth.Permissions() {
		if snap.Allows(anonymous, perm) {
			t.Errorf("Allows(zero principal, %q) = true, want false", perm)
		}
	}
}

func TestR2_BindingsEnforceExactAndWildcardScopes(t *testing.T) {
	snapshot := compile(t, 1, fixedPolicyYAML)
	reader := principal(t, snapshot, "bydb-reader", "reader-secret")
	if !snapshot.Allows(reader, auth.PermissionDataRead, "sw_metric") {
		t.Fatal("exact reader binding did not allow sw_metric")
	}
	if snapshot.Allows(reader, auth.PermissionDataRead, "sw_record") {
		t.Fatal("exact reader binding allowed an unrelated group")
	}
	writer := principal(t, snapshot, "bydb-writer", "writer-secret")
	for _, group := range []string{"sw_metric", "created-after-policy-load"} {
		if !snapshot.Allows(writer, auth.PermissionDataWrite, group) {
			t.Errorf("wildcard writer binding did not allow %q", group)
		}
	}
}

// TestR2_InvalidPolicyIsRejected proves that a configuration the compiler cannot trust is
// rejected outright rather than compiled into a partial grant set. Each case names a
// distinct way the file can lie about its own model.
func TestR2_InvalidPolicyIsRejected(t *testing.T) {
	// Anchor the test against a compiler that rejects everything: the valid fixture must
	// still compile, so "rejected" means "rejected for this defect" and not "never accepts".
	compile(t, 1, fixedPolicyYAML)

	for _, tc := range []struct {
		name string
		raw  string
	}{
		{
			name: "permission outside the closed vocabulary",
			raw: "users:\n  - username: \"a\"\n    password: \"b\"\nrbac:\n  enabled: true\n" +
				"  roles:\n    r:\n      permissions: [\"cluster:teleport\"]\n  bindings: []\n",
		},
		{
			name: "binding names a role that is not declared",
			raw: "users:\n  - username: \"a\"\n    password: \"b\"\nrbac:\n  enabled: true\n" +
				"  roles: {}\n  bindings:\n    - principal: \"a\"\n      role: \"ghost\"\n      groups: [\"*\"]\n",
		},
		{
			name: "binding names a user that is not declared",
			raw: "users:\n  - username: \"a\"\n    password: \"b\"\nrbac:\n  enabled: true\n" +
				"  roles:\n    r:\n      permissions: [\"cluster:read\"]\n  bindings:\n    - principal: \"ghost\"\n      role: \"r\"\n      groups: [\"*\"]\n",
		},
		{
			name: "duplicate role name",
			raw: "users:\n  - username: \"a\"\n    password: \"b\"\nrbac:\n  enabled: true\n" +
				"  roles:\n    r:\n      permissions: [\"cluster:read\"]\n    r:\n      permissions: [\"cluster:admin\"]\n  bindings: []\n",
		},
		{
			name: "duplicate user name",
			raw:  "users:\n  - username: \"a\"\n    password: \"b\"\n  - username: \"a\"\n    password: \"c\"\nrbac:\n  enabled: true\n  roles: {}\n  bindings: []\n",
		},
		{
			name: "built-in role override",
			raw: "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  roles:\n" +
				"    reader:\n      permissions: [data:write]\n  bindings: []\n",
		},
		{
			name: "duplicate permission",
			raw: "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  roles:\n" +
				"    monitor:\n      permissions: [cluster:read, cluster:read]\n  bindings: []\n",
		},
		{
			name: "duplicate group",
			raw: "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  roles: {}\n  bindings:\n" +
				"    - principal: a\n      role: reader\n      groups: [alpha, alpha]\n",
		},
		{
			name: "equivalent binding",
			raw: "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  roles: {}\n  bindings:\n" +
				"    - principal: a\n      role: reader\n      groups: [alpha, beta]\n" +
				"    - principal: a\n      role: reader\n      groups: [beta, alpha]\n",
		},
		{
			name: "wildcard mixed with group",
			raw: "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  roles: {}\n  bindings:\n" +
				"    - principal: a\n      role: reader\n      groups: ['*', alpha]\n",
		},
		{
			name: "cluster permission with exact scope",
			raw: "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  roles:\n" +
				"    monitor:\n      permissions: [cluster:read]\n  bindings:\n" +
				"    - principal: a\n      role: monitor\n      groups: [alpha]\n",
		},
		{
			name: "unknown RBAC field",
			raw:  "users:\n  - username: a\n    password: b\nrbac:\n  enabled: true\n  typo: true\n  roles: {}\n  bindings: []\n",
		},
		{
			name: "enabled without user",
			raw:  "users: []\nrbac:\n  enabled: true\n  roles: {}\n  bindings: []\n",
		},
		{name: "truncated document", raw: truncatedYAML},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := auth.CompileSnapshot(1, []byte(tc.raw))
			if err == nil {
				t.Fatalf("CompileSnapshot accepted a policy with %s, want ErrInvalidPolicy", tc.name)
			}
			if !errors.Is(err, auth.ErrInvalidPolicy) {
				t.Errorf("CompileSnapshot error = %v, want it to wrap ErrInvalidPolicy", err)
			}
			if snap != nil {
				t.Error("CompileSnapshot returned a snapshot alongside an error, want nil")
			}
		})
	}
}

// TestR5_RejectedReloadRetainsLastKnownGood proves R5: a reload that cannot be compiled
// leaves the previous revision in force, and a snapshot a caller already holds keeps
// answering from the revision it was taken at even after a later reload replaces it.
//
// The truncated document is this file's crash case: it is what a reader observes when it
// wakes on the fsnotify event between the writer's open and its final write. The invariant
// is that such a read can never disarm authorization.
func TestR5_RejectedReloadRetainsLastKnownGood(t *testing.T) {
	dir := t.TempDir()
	path := writeSecurityFile(t, dir, "security.yaml", fixedPolicyYAML)

	reloader := auth.InitAuthReloader()
	if err := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-contract-test")); err != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want it to accept the fixed policy", path, err)
	}
	if err := reloader.Start(); err != nil {
		t.Fatalf("Start() = %v, want the watcher to start", err)
	}
	defer reloader.Stop()

	held := reloader.CurrentSnapshot()
	if held == nil {
		t.Fatal("CurrentSnapshot() = nil after a successful load, want the loaded snapshot")
	}
	if !held.RBACEnabled() {
		t.Fatal("CurrentSnapshot().RBACEnabled() = false, want true for the fixed enabled policy")
	}
	goodRevision := held.Revision()
	monitor := principal(t, held, "bydb-monitor", "monitor-secret")

	// A malformed rewrite must be refused. The revision must not move and the held
	// snapshot must not change its answers.
	if err := os.WriteFile(path, []byte(truncatedYAML), 0o600); err != nil {
		t.Fatalf("rewriting %s: %v", path, err)
	}
	// The reloader debounces file events; wait past the debounce before sampling.
	time.Sleep(2 * time.Second)

	after := reloader.CurrentSnapshot()
	if after == nil {
		t.Fatal("CurrentSnapshot() = nil after a rejected reload, want the previous snapshot")
	}
	if after.Revision() != goodRevision {
		t.Errorf("Revision() = %d after a rejected reload, want it to stay at %d", after.Revision(), goodRevision)
	}
	if _, ok := after.Authenticate("bydb-monitor", "monitor-secret"); !ok {
		t.Error("the previous revision stopped authenticating bydb-monitor after a rejected reload, want it retained")
	}
	if !after.Allows(monitor, auth.PermissionClusterRead, "*") {
		t.Error("the previous revision stopped granting cluster:read to bydb-monitor after a rejected reload, want it retained")
	}
	if _, ok := after.Authenticate("a", "b"); ok {
		t.Error(`the rejected file's user "a" authenticates, want the rejected revision to have been discarded entirely`)
	}
}

// TestR5_AcceptedReloadAdvancesRevisionAtomically proves the other half of R5: an accepted
// reload publishes a new snapshot at a strictly higher revision, and the snapshot a caller
// took before the reload still answers from the old revision. That is what makes a
// decision taken mid-request self-consistent.
func TestR5_AcceptedReloadAdvancesRevisionAtomically(t *testing.T) {
	dir := t.TempDir()
	path := writeSecurityFile(t, dir, "security.yaml", fixedPolicyYAML)

	reloader := auth.InitAuthReloader()
	if err := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-contract-test")); err != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want it to accept the fixed policy", path, err)
	}
	if err := reloader.Start(); err != nil {
		t.Fatalf("Start() = %v, want the watcher to start", err)
	}
	defer reloader.Stop()

	before := reloader.CurrentSnapshot()
	if before == nil {
		t.Fatal("CurrentSnapshot() = nil after a successful load, want the loaded snapshot")
	}
	beforeRevision := before.Revision()
	monitor := principal(t, before, "bydb-monitor", "monitor-secret")

	// Demote `monitor` to hold nothing. The revision must advance and the new snapshot
	// must reflect the demotion, while `before` must not.
	demoted := `
users:
  - username: "bydb-monitor"
    password: "monitor-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: []
  bindings:
    - principal: "bydb-monitor"
      role: "monitor"
      groups: ["*"]
`
	if err := os.WriteFile(path, []byte(demoted), 0o600); err != nil {
		t.Fatalf("rewriting %s: %v", path, err)
	}
	select {
	case <-reloader.GetUpdateChannel():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the reloader to accept the rewritten policy")
	}

	after := reloader.CurrentSnapshot()
	if after == nil {
		t.Fatal("CurrentSnapshot() = nil after an accepted reload, want the new snapshot")
	}
	if after.Revision() <= beforeRevision {
		t.Errorf("Revision() = %d after an accepted reload, want it above the previous %d", after.Revision(), beforeRevision)
	}
	demotedMonitor := principal(t, after, "bydb-monitor", "monitor-secret")
	if after.Allows(demotedMonitor, auth.PermissionClusterRead, "*") {
		t.Error("the new revision still grants cluster:read to the demoted bydb-monitor, want it revoked")
	}
	if !before.Allows(monitor, auth.PermissionClusterRead, "*") {
		t.Error("the snapshot taken before the reload changed its answer, want an immutable view of its own revision")
	}
	if before.Revision() != beforeRevision {
		t.Errorf("the snapshot taken before the reload reports Revision() = %d, want it pinned at %d", before.Revision(), beforeRevision)
	}
}

type recordingPolicyObserver struct {
	results   []string
	revisions []uint64
	mu        sync.Mutex
}

func (o *recordingPolicyObserver) ObservePolicyReload(result string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.results = append(o.results, result)
}

func (o *recordingPolicyObserver) SetPolicyRevision(revision uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.revisions = append(o.revisions, revision)
}

func (o *recordingPolicyObserver) snapshot() ([]string, []uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string{}, o.results...), append([]uint64{}, o.revisions...)
}

func TestR5_ReloadObservabilityTracksPublicationAndRejection(t *testing.T) {
	dir := t.TempDir()
	path := writeSecurityFile(t, dir, "security.yaml", fixedPolicyYAML)
	reloader := auth.InitAuthReloader()
	if configureErr := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-observer-test")); configureErr != nil {
		t.Fatalf("ConfigAuthReloader() error = %v", configureErr)
	}
	observer := &recordingPolicyObserver{}
	reloader.SetPolicyObserver(observer)
	results, revisions := observer.snapshot()
	if len(results) != 1 || results[0] != auth.PolicyReloadSuccess {
		t.Fatalf("initial reload results = %v, want [%q]", results, auth.PolicyReloadSuccess)
	}
	if len(revisions) != 1 || revisions[0] != 1 {
		t.Fatalf("initial revisions = %v, want [1]", revisions)
	}
	if startErr := reloader.Start(); startErr != nil {
		t.Fatalf("Start() error = %v", startErr)
	}
	defer reloader.Stop()
	if writeErr := os.WriteFile(path, []byte(truncatedYAML), 0o600); writeErr != nil {
		t.Fatalf("writing invalid reload: %v", writeErr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		results, revisions = observer.snapshot()
		if len(results) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(results) != 2 || results[1] != auth.PolicyReloadFailure {
		t.Fatalf("reload results = %v, want [success failure]", results)
	}
	if len(revisions) != 1 || revisions[0] != 1 {
		t.Fatalf("revisions after rejected reload = %v, want unchanged [1]", revisions)
	}
}
