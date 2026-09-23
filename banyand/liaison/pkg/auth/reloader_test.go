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

package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestConfigMarshalOmitsRuntimeFields(t *testing.T) {
	configuration := Config{
		Users:             []User{{Username: "alice", Password: "secret"}},
		Enabled:           true,
		HealthAuthEnabled: true,
	}
	encoded, marshalErr := yaml.Marshal(configuration)
	if marshalErr != nil {
		t.Fatalf("yaml.Marshal() error = %v", marshalErr)
	}
	for _, runtimeField := range []string{"Enabled", "HealthAuthEnabled"} {
		if strings.Contains(string(encoded), runtimeField) {
			t.Errorf("yaml.Marshal() output contains runtime field %q: %s", runtimeField, encoded)
		}
	}
	if _, compileErr := CompileSnapshot(1, encoded); compileErr != nil {
		t.Fatalf("CompileSnapshot() rejected marshaled Config: %v", compileErr)
	}
}

func writeConfigFile(t *testing.T, dir, filename, content string) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	return path
}

func TestLoadConfigAndAuthCheck(t *testing.T) {
	dir := t.TempDir()
	configYAML := `
users:
  - username: "alice"
    password: "secret"
  - username: "bob"
    password: "hunter2"
`
	path := writeConfigFile(t, dir, "auth.yaml", configYAML)
	// init reloader
	ar := InitAuthReloader()
	err := ar.loadConfig(path)
	if err != nil {
		t.Fatalf("expected loadConfig success, got error: %v", err)
	}
	cfg := ar.GetConfig()
	if len(cfg.Users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(cfg.Users))
	}
	log := logger.GetLogger("auth-test")
	if err := ar.ConfigAuthReloader(path, false, log); err != nil {
		t.Fatalf("ConfigAuthReloader failed: %v", err)
	}

	if !ar.CheckUsernameAndPassword("alice", "secret") {
		t.Errorf("expected alice/secret to be valid")
	}
	if ar.CheckUsernameAndPassword("alice", "wrong") {
		t.Errorf("expected alice/wrong to be invalid")
	}
	if ar.CheckUsernameAndPassword("notexist", "secret") {
		t.Errorf("expected non-existent user to fail")
	}
}

func TestReloaderUpdatesOnFileChange(t *testing.T) {
	dir := t.TempDir()
	initialYAML := `
users:
  - username: "alice"
    password: "secret"
`
	path := writeConfigFile(t, dir, "auth.yaml", initialYAML)

	ar := InitAuthReloader()
	log := logger.GetLogger("auth-test")
	if err := ar.ConfigAuthReloader(path, false, log); err != nil {
		t.Fatalf("ConfigAuthReloader failed: %v", err)
	}

	if err := ar.Start(); err != nil {
		t.Fatalf("failed to start reloader: %v", err)
	}
	defer ar.Stop()

	if !ar.CheckUsernameAndPassword("alice", "secret") {
		t.Fatalf("expected alice/secret to be valid before update")
	}

	updatedYAML := `
users:
  - username: "bob"
    password: "hunter2"
`
	err := os.WriteFile(path, []byte(updatedYAML), 0o600)
	if err != nil {
		t.Fatalf("failed to update config file: %v", err)
	}

	select {
	case <-ar.GetUpdateChannel():
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for update channel notification")
	}

	if ar.CheckUsernameAndPassword("alice", "secret") {
		t.Errorf("alice should no longer be valid after update")
	}
	if !ar.CheckUsernameAndPassword("bob", "hunter2") {
		t.Errorf("expected bob/hunter2 to be valid after update")
	}
}

func TestReloaderUpdatesAfterAtomicReplacement(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, "auth.yaml", `
users:
  - username: "alice"
    password: "secret"
`)

	reloader := InitAuthReloader()
	log := logger.GetLogger("auth-atomic-replace-test")
	if configureErr := reloader.ConfigAuthReloader(path, false, log); configureErr != nil {
		t.Fatalf("ConfigAuthReloader failed: %v", configureErr)
	}
	if startErr := reloader.Start(); startErr != nil {
		t.Fatalf("failed to start reloader: %v", startErr)
	}
	defer reloader.Stop()

	replacementPath := writeConfigFile(t, dir, "auth.next.yaml", `
users:
  - username: "bob"
    password: "hunter2"
`)
	if renameErr := os.Rename(replacementPath, path); renameErr != nil {
		t.Fatalf("failed to replace config file atomically: %v", renameErr)
	}

	select {
	case <-reloader.GetUpdateChannel():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update after atomic config replacement")
	}
	if reloader.CheckUsernameAndPassword("alice", "secret") {
		t.Error("alice should no longer be valid after atomic replacement")
	}
	if !reloader.CheckUsernameAndPassword("bob", "hunter2") {
		t.Error("expected bob/hunter2 to be valid after atomic replacement")
	}
}

func TestReloaderUpdatesWhenSymlinkTargetChanges(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.yaml")
	linkPath := filepath.Join(dir, "auth.yaml")
	initialYAML := `
users:
  - username: "alice"
    password: "old"
rbac:
  enabled: true
  bindings:
    - principal: "alice"
      role: "writer"
      groups: ["payments"]
`
	if writeErr := os.WriteFile(targetPath, []byte(initialYAML), 0o600); writeErr != nil {
		t.Fatalf("writing symlink target: %v", writeErr)
	}
	if linkErr := os.Symlink(targetPath, linkPath); linkErr != nil {
		t.Fatalf("creating auth symlink: %v", linkErr)
	}

	reloader := InitAuthReloader()
	log := logger.GetLogger("auth-symlink-target-test")
	if configureErr := reloader.ConfigAuthReloader(linkPath, false, log); configureErr != nil {
		t.Fatalf("ConfigAuthReloader failed: %v", configureErr)
	}
	if startErr := reloader.Start(); startErr != nil {
		t.Fatalf("Start failed: %v", startErr)
	}
	defer reloader.Stop()

	initialRevision := reloader.CurrentSnapshot().Revision()
	if !reloader.CheckUsernameAndPassword("alice", "old") {
		t.Fatal("expected alice/old to authenticate before target update")
	}

	updatedYAML := `
users:
  - username: "alice"
    password: "new"
rbac:
  enabled: true
  bindings: []
`
	if writeErr := os.WriteFile(targetPath, []byte(updatedYAML), 0o600); writeErr != nil {
		t.Fatalf("updating symlink target: %v", writeErr)
	}

	select {
	case <-reloader.GetUpdateChannel():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reload after symlink target write")
	}

	if reloader.CurrentSnapshot().Revision() <= initialRevision {
		t.Fatalf("revision = %d after target write, want greater than %d", reloader.CurrentSnapshot().Revision(), initialRevision)
	}
	if reloader.CheckUsernameAndPassword("alice", "old") {
		t.Error("alice/old should be rejected after target password rotation")
	}
	if !reloader.CheckUsernameAndPassword("alice", "new") {
		t.Error("expected alice/new to authenticate after target update")
	}
	principal, ok := reloader.CurrentSnapshot().Authenticate("alice", "new")
	if !ok {
		t.Fatal("Authenticate(alice, new) failed after target update")
	}
	if reloader.CurrentSnapshot().Allows(principal, PermissionDataWrite, "payments") {
		t.Error("writer grant on payments should be cleared after bindings removed")
	}
}
