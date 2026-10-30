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

package stream

import (
	"fmt"
	"os"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/test"
)

// console holds what the package's tests print to stderr.
var console *test.CapturedStderr

// TestMain replaces os.Stderr once, before any test runs. zerolog's console
// writer reads os.Stderr on every write, so replacing it while a goroutine an
// earlier test started still logs is a data race; here nothing logs yet. A
// test that asserts on the console reads the part printed since it began.
// When a test fails, what was printed is written to the real stderr.
func TestMain(m *testing.M) {
	stderr := os.Stderr
	var err error
	console, err = test.CaptureStderr()
	if err != nil {
		fmt.Fprintln(stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if code != 0 {
		_, _ = stderr.WriteString(console.String())
	}
	os.Exit(code)
}
