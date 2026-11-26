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
	"fmt"
	"io"
	"slices"
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
		if got := lineConfirm(strings.NewReader(answer), &out)(context.Background(), "take over? [y/N] "); got != want {
			t.Fatalf("answer %q: got %v, want %v", answer, got, want)
		}
		if out.String() != "take over? [y/N] " {
			t.Fatalf("the question must be printed once, got %q", out.String())
		}
	}
}

// Answers typed ahead for several questions are not swallowed by the first one: each question
// of one asker reads its own line of the same input, and the end of the input is a no.
func TestAskYesKeepsAnswersTypedAhead(t *testing.T) {
	var out bytes.Buffer
	ask := lineConfirm(strings.NewReader("y\nn\nyes\n"), &out)
	var got []bool
	for range 4 {
		got = append(got, ask(context.Background(), "resume? [y/N] "))
	}
	if want := []bool{true, false, true, false}; !slices.Equal(got, want) {
		t.Fatalf("answers %v, want %v", got, want)
	}
	if out.String() != strings.Repeat("resume? [y/N] ", 4) {
		t.Fatalf("every question must be printed once, got %q", out.String())
	}
}

// Ctrl-C while the question waits is a no, though the read itself cannot be interrupted.
func TestAskYesCanceledIsNo(t *testing.T) {
	in, w := io.Pipe()
	defer w.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	var out bytes.Buffer
	if lineConfirm(in, &out)(ctx, "take over? [y/N] ") {
		t.Fatal("a canceled question must be a no")
	}
}

// terminals makes isTTY take exactly the given streams for terminals, until the test ends.
func terminals(t *testing.T, tty ...any) {
	t.Helper()
	old := isTTY
	isTTY = func(v any) bool {
		for _, x := range tty {
			if x == v {
				return true
			}
		}
		return false
	}
	t.Cleanup(func() { isTTY = old })
}

// Only an operator at a terminal is asked: a pipe, a file or a buffer is not.
func TestTerminalConfirmNeedsTerminals(t *testing.T) {
	in, stdout, stderr := strings.NewReader("y\n"), &bytes.Buffer{}, &bytes.Buffer{}
	for name, tty := range map[string][]any{
		"none":              nil,
		"stdout and stderr": {stdout, stderr},
		"only stdin":        {in},
	} {
		terminals(t, tty...)
		if ask, _ := terminalConfirm(in, stdout, stderr); ask != nil {
			t.Fatalf("%s: no question without a terminal to ask on", name)
		}
	}
}

// With stdin and stdout at a terminal the question and its summary stay on stdout.
func TestTerminalConfirmOnStdout(t *testing.T) {
	in, stdout, stderr := strings.NewReader("y\n"), &bytes.Buffer{}, &bytes.Buffer{}
	terminals(t, in, stdout, stderr)
	ask, wrap := terminalConfirm(in, stdout, stderr)
	fmt.Fprintln(wrap(stdout), "SUMMARY")
	if ask == nil || !ask(context.Background(), "Resume? [y/N] ") {
		t.Fatal("y must be a yes")
	}
	if got := stdout.String(); got != "SUMMARY\nResume? [y/N] " {
		t.Fatalf("stdout = %q", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want nothing", stderr.String())
	}
}

// With stdout redirected and stderr at a terminal the question goes to stderr, after what the run
// printed so far; stdout keeps what it held.
func TestTerminalConfirmOnStderr(t *testing.T) {
	in, stdout, stderr := strings.NewReader("y\nn\n"), &bytes.Buffer{}, &bytes.Buffer{}
	terminals(t, in, stderr)
	ask, wrap := terminalConfirm(in, stdout, stderr)
	if ask == nil {
		t.Fatal("stderr is a terminal: the operator can be asked")
	}
	log := wrap(stdout)
	fmt.Fprintln(log, "RESUME")
	fmt.Fprintln(wrap(stdout), "DONE")
	if !ask(context.Background(), "Resume? [y/N] ") {
		t.Fatal("y must be a yes")
	}
	if got := stderr.String(); got != "RESUME\nDONE\nResume? [y/N] " {
		t.Fatalf("stderr = %q", got)
	}
	if got := stdout.String(); got != "RESUME\nDONE\n" {
		t.Fatalf("stdout = %q, want the lines the run printed", got)
	}
	// The summary is shown once: a later question comes alone.
	stderr.Reset()
	if ask(context.Background(), "again? ") {
		t.Fatal("n must be a no")
	}
	if got := stderr.String(); got != "again? " {
		t.Fatalf("stderr = %q", got)
	}
}

// With json or yaml output the information is on stderr: a terminal there is asked as before, a
// redirected one is not, whatever stdout is.
func TestTerminalConfirmStructured(t *testing.T) {
	in, stderr := strings.NewReader("y\n"), &bytes.Buffer{}
	terminals(t, in, stderr)
	ask, wrap := terminalConfirm(in, stderr, stderr)
	fmt.Fprintln(wrap(stderr), "SUMMARY")
	if ask == nil || !ask(context.Background(), "Resume? [y/N] ") {
		t.Fatal("y must be a yes")
	}
	if got := stderr.String(); got != "SUMMARY\nResume? [y/N] " {
		t.Fatalf("stderr = %q", got)
	}
	terminals(t, in)
	if ask, _ = terminalConfirm(in, stderr, stderr); ask != nil {
		t.Fatal("no question when stderr is redirected")
	}
}
