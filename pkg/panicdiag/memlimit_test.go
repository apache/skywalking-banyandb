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

package panicdiag

import (
	"errors"
	"testing"
)

func TestApplyGoMemLimit(t *testing.T) {
	t.Helper()

	origCgroup := cgroupMemLimit
	origSet := setMemoryLimit
	defer func() {
		cgroupMemLimit = origCgroup
		setMemoryLimit = origSet
	}()

	const cgBytes = int64(4 * 1024 * 1024 * 1024) // 4 GiB

	var applied int64
	setMemoryLimit = func(n int64) int64 {
		applied = n
		return 0
	}
	cgroupMemLimit = func() (int64, error) {
		return cgBytes, nil
	}

	got, err := ApplyGoMemLimit(90)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := cgBytes * 90 / 100
	if got != want {
		t.Fatalf("returned limit: got %d want %d", got, want)
	}
	if applied != want {
		t.Fatalf("SetMemoryLimit called with %d, want %d", applied, want)
	}
}

func TestApplyGoMemLimitNoCgroup(t *testing.T) {
	t.Helper()

	origCgroup := cgroupMemLimit
	origSet := setMemoryLimit
	defer func() {
		cgroupMemLimit = origCgroup
		setMemoryLimit = origSet
	}()

	setMemoryLimit = func(_ int64) int64 {
		t.Fatal("SetMemoryLimit should not be called when cgroup limit is unavailable")
		return 0
	}
	cgroupMemLimit = func() (int64, error) {
		return 0, errors.New("no cgroup")
	}

	got, err := ApplyGoMemLimit(90)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestApplyGoMemLimitClampsPct(t *testing.T) {
	t.Helper()

	origCgroup := cgroupMemLimit
	origSet := setMemoryLimit
	defer func() {
		cgroupMemLimit = origCgroup
		setMemoryLimit = origSet
	}()

	const cgBytes = int64(1024)
	var applied int64
	setMemoryLimit = func(n int64) int64 {
		applied = n
		return 0
	}
	cgroupMemLimit = func() (int64, error) { return cgBytes, nil }

	// pct > 99 should clamp to 99
	if _, err := ApplyGoMemLimit(150); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied != cgBytes*99/100 {
		t.Fatalf("expected %d after clamping to 99%%, got %d", cgBytes*99/100, applied)
	}

	// pct < 1 should clamp to 1
	applied = 0
	if _, err := ApplyGoMemLimit(0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied != cgBytes*1/100 {
		t.Fatalf("expected %d after clamping to 1%%, got %d", cgBytes*1/100, applied)
	}
}
