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
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

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
