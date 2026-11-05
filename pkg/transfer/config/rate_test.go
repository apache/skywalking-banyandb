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

package config

import (
	"math"
	"strings"
	"testing"
)

// TestParseRate covers every accepted rate form and rejection.
func TestParseRate(t *testing.T) {
	const mib = 1 << 20
	accepted := []struct {
		raw  string
		kind RateKind
		want float64
	}{
		{"0", RateBytes, 0},
		{"0", RateRows, 0},
		{"0/min", RateRows, 0},
		{"52428800", RateBytes, 50 * mib},
		{"50MiB/s", RateBytes, 50 * mib},
		{"50mib/s", RateBytes, 50 * mib},
		{"50MiB", RateBytes, 50 * mib},
		{"3GiB/min", RateBytes, 3 * (1 << 30) / 60.0},
		{"1TiB/h", RateBytes, (1 << 40) / 3600.0},
		{"10MB/s", RateBytes, 10_000_000},
		{"50000", RateRows, 50000},
		{"3000000/min", RateRows, 50000},
		{"3600/h", RateRows, 1},
	}
	for _, c := range accepted {
		got, err := ParseRate(c.raw, c.kind)
		if err != nil || math.Abs(got-c.want) > 1e-9 {
			t.Fatalf("ParseRate(%q, %s) = %v, %v; want %v", c.raw, c.kind, got, err, c.want)
		}
	}
	rejected := []struct {
		raw  string
		want string
		kind RateKind
	}{
		{"-1", "negative", RateBytes},
		{"-5/s", "negative", RateRows},
		{"50MiB/m", `use "min"`, RateBytes},
		{"100/m", `use "min"`, RateRows},
		{"50MiB/day", "unknown period", RateBytes},
		{"50MiB/", "unknown period", RateBytes},
		{"50MiB/s", "not a byte unit", RateRows},
		{"50000KB", "not a byte unit", RateRows},
		{"1.5", "plain integer", RateRows},
		{"1KiB/h", "below 1", RateBytes},
		{"0.5", "below 1 byte", RateBytes},
		{"0.9B/s", "below 1 byte", RateBytes},
		{"59/min", "below 1", RateRows},
		{"", "empty", RateBytes},
		{"fast", "invalid size", RateBytes},
	}
	for _, c := range rejected {
		if _, err := ParseRate(c.raw, c.kind); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("ParseRate(%q, %s) = %v; want an error containing %q", c.raw, c.kind, err, c.want)
		}
	}
	// The normalized value is printed so the operator sees the unit mistake.
	if _, err := ParseRate("1KiB/h", RateBytes); err == nil || !strings.Contains(err.Error(), "0.28") {
		t.Fatalf("the error must show the normalized value: %v", err)
	}
}

func TestParseSize(t *testing.T) {
	for raw, want := range map[string]uint64{"0": 0, "512MiB": 512 << 20, "1048576": 1 << 20, "1GB": 1_000_000_000} {
		if got, err := ParseSize(raw); err != nil || got != want {
			t.Fatalf("ParseSize(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, bad := range []string{"-1", "big", "", "1XB", "20000PiB", "1e30PiB"} {
		if _, err := ParseSize(bad); err == nil {
			t.Fatalf("ParseSize(%q) must fail", bad)
		}
	}
	for _, raw := range []string{"0.5", "0.5B", "0.0001KB"} {
		if _, err := ParseSize(raw); err == nil || !strings.Contains(err.Error(), "below 1 byte") {
			t.Fatalf("ParseSize(%q) must refuse a size below one byte, got %v", raw, err)
		}
	}
}
