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

package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestAskYes(t *testing.T) {
	for _, c := range []struct {
		answer string
		want   bool
	}{
		{"y\n", true},
		{"Y\n", true},
		{"yes\n", true},
		{" YES \n", true},
		{"yes", true},
		{"n\n", false},
		{"N\n", false},
		{"\n", false},
		{"", false},
		{"sure\n", false},
	} {
		answer, want := c.answer, c.want
		var out bytes.Buffer
		if got := askYes(context.Background(), strings.NewReader(answer), &out, "take over? [y/N] "); got != want {
			t.Fatalf("answer %q: got %v, want %v", answer, got, want)
		}
		if out.String() != "take over? [y/N] " {
			t.Fatalf("the question must be printed once, got %q", out.String())
		}
	}
}

// Ctrl-C while the question waits is a no, though the read itself cannot be interrupted.
func TestAskYesCanceledIsNo(t *testing.T) {
	in, w := io.Pipe()
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	var out bytes.Buffer
	if askYes(ctx, in, &out, "take over? [y/N] ") {
		t.Fatal("a canceled question must be a no")
	}
}

// Only an operator at a terminal is asked: a pipe, a file or a buffer is not.
func TestTerminalConfirmNeedsTerminals(t *testing.T) {
	if terminalConfirm(strings.NewReader("y\n"), &bytes.Buffer{}) != nil {
		t.Fatal("no question without a terminal")
	}
}
