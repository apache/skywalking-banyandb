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

// Package leakcheck fails a test binary that finishes with resources still held.
package leakcheck

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// settle is how long a check may take to drain before it is reported: releases
// that run on other goroutines land shortly after the last test returns.
const settle = 5 * time.Second

// Check returns a description of what is still held, or "" when nothing is.
type Check func() string

// Main runs the tests of m and, when they all pass, fails the binary if any check
// still reports a leak after it had time to settle. It is meant for TestMain:
//
//	func TestMain(m *testing.M) { leakcheck.Main(m, leakcheck.Counter("owner pins", vectorized.OutstandingOwnerPins)) }
//
// Set BYDB_LEAK_STACKS=1 to track where pooled objects were acquired.
func Main(m *testing.M, checks ...Check) {
	if os.Getenv("BYDB_LEAK_STACKS") == "1" {
		pool.EnableStackTracking(true)
	}
	code := m.Run()
	if code == 0 {
		if report := Leaked(checks...); report != "" {
			fmt.Fprintln(os.Stderr, report)
			code = 1
		}
	}
	os.Exit(code)
}

// Leaked waits for every check to drain and returns the report of those that did
// not, or "" when all are clean.
func Leaked(checks ...Check) string {
	deadline := time.Now().Add(settle)
	for {
		var reports []string
		for _, c := range checks {
			if r := c(); r != "" {
				reports = append(reports, r)
			}
		}
		if len(reports) == 0 {
			return ""
		}
		if time.Now().After(deadline) {
			return "resources still held at the end of the test binary: " + strings.Join(reports, "; ")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Counter is a check on a count that must return to zero.
func Counter(name string, count func() int64) Check {
	return func() string {
		if n := count(); n != 0 {
			return fmt.Sprintf("%s=%d", name, n)
		}
		return ""
	}
}

// Pools is a check that every pool whose name starts with one of prefixes has
// nothing checked out. A pool whose objects are also created outside it and then
// released into it counts below zero, so use it only for pools every object of
// which is taken from the pool. It passes trivially in slim builds.
func Pools(prefixes ...string) Check {
	return func() string {
		var leaked []string
		for name, refs := range pool.AllRefsCount() {
			if refs != 0 && hasPrefix(name, prefixes) {
				leaked = append(leaked, fmt.Sprintf("%s=%d", name, refs))
			}
		}
		sort.Strings(leaked)
		return strings.Join(leaked, ", ")
	}
}

func hasPrefix(name string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}
