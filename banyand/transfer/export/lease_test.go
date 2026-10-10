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

package export

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLeaseRoundTripAndState(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	l := Lease{SessionID: "9f2a", StartedAt: now.UnixNano(), ExpiresAt: now.Add(time.Hour).UnixNano(), LastHeartbeatAt: now.UnixNano()}
	if err := writeLease(dir, l); err != nil {
		t.Fatal(err)
	}
	got, err := readLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != l {
		t.Fatalf("readLease() = %+v, want %+v", got, l)
	}
	if got.expired(now.Add(30 * time.Minute)) {
		t.Fatal("lease must not be expired before expiresAt")
	}
	if !got.expired(now.Add(2 * time.Hour)) {
		t.Fatal("lease must be expired after expiresAt")
	}
	if _, err := readLease(t.TempDir()); !errors.Is(err, errNoLease) {
		t.Fatalf("missing lease must be errNoLease, got %v", err)
	}
}

func TestReadLease_IgnoresKeysOfEarlierBuilds(t *testing.T) {
	dir := t.TempDir()
	raw := []byte(`{"sessionId":"9f2a","client":"legacy-client","catalogs":["CATALOG_STREAM"],` +
		`"startedAt":1,"expiresAt":3,"lastHeartbeatAt":2}`)
	if err := os.WriteFile(filepath.Join(dir, LeaseFileName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := (Lease{SessionID: "9f2a", StartedAt: 1, ExpiresAt: 3, LastHeartbeatAt: 2}); got != want {
		t.Fatalf("readLease() = %+v, want %+v", got, want)
	}
}

func TestValidateSessionID(t *testing.T) {
	for _, ok := range []string{"a", "9f2ac1d4", "0123456789abcdef0123456789abcdef"} {
		if err := validateSessionID(ok); err != nil {
			t.Fatalf("%q must be accepted: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "/../../data", "ABC", "9f2a-1", "..", "x y"} {
		if err := validateSessionID(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}

func TestReadLease_CorruptedFileIsNotErrNoLease(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, LeaseFileName), []byte(`{broken`))
	_, err := readLease(dir)
	if err == nil {
		t.Fatal("corrupted lease file must return an error")
	}
	if errors.Is(err, errNoLease) {
		t.Fatalf("corrupted lease file must not be errNoLease, got %v", err)
	}
}
