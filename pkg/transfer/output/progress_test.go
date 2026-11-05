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

package output

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// textLine is a live line of fixed text.
type textLine string

func (l textLine) LiveLine(time.Time) string { return string(l) }

func terminalOf(width, height int) func() (int, int) {
	return func() (int, int) { return width, height }
}

// More live lines than the terminal has rows would scroll some off the screen, where they can
// no longer be erased: the live area keeps below the height and counts the rest.
func TestProgress_LiveAreaFitsTheTerminalHeight(t *testing.T) {
	var out bytes.Buffer
	p := NewProgress(&out, terminalOf(200, 4), "group(s) importing")
	for _, l := range []textLine{"n:1", "n:2", "n:3", "n:4"} {
		p.Add(l)
	}
	out.Reset()
	p.Redraw()
	got := out.String()
	if !strings.HasPrefix(got, "\r"+strings.Repeat(eraseLine, 3)) || strings.Count(got, "\n") != 3 ||
		!strings.Contains(got, "n:2") || strings.Contains(got, "n:3") || !strings.HasSuffix(got, "  ... 2 more group(s) importing\n") {
		t.Fatalf("got %q", got)
	}
	if p.drawn != 3 {
		t.Fatalf("drawn = %d, want 3", p.drawn)
	}
}

// A live line never wraps: it is cut to the terminal width, and an unknown width counts as
// defaultWidth columns.
func TestProgress_LiveLineFitsTheTerminal(t *testing.T) {
	var out bytes.Buffer
	p := NewProgress(&out, terminalOf(0, 0), "")
	p.Add(textLine(strings.Repeat("n", 100)))
	if got := strings.TrimSuffix(strings.TrimPrefix(out.String(), "\r"), "\n"); len([]rune(got)) != defaultWidth-1 {
		t.Fatalf("got %d runes: %q", len([]rune(got)), got)
	}
}

// A fixed line goes above the live lines, which are drawn again below it; removing a line
// erases it, and closing erases what is left.
func TestProgress_FixedLinesGoAboveTheLiveLines(t *testing.T) {
	var out bytes.Buffer
	p := NewProgress(&out, terminalOf(200, 50), "")
	a, b := textLine("a"), textLine("b")
	p.Add(a)
	p.Add(b)
	out.Reset()
	p.Update(func() string { return "done x\n" })
	if want := "\r" + strings.Repeat(eraseLine, 2) + "done x\na\nb\n"; out.String() != want {
		t.Fatalf("got %q, want %q", out.String(), want)
	}
	out.Reset()
	p.Update(func() string { return "" })
	if out.String() != "" {
		t.Fatalf("an update without a fixed line redrew: %q", out.String())
	}
	p.Remove(a, "✓ a\n")
	if want := "\r" + strings.Repeat(eraseLine, 2) + "✓ a\nb\n"; out.String() != want {
		t.Fatalf("got %q, want %q", out.String(), want)
	}
	out.Reset()
	p.Close()
	if out.String() != "\r"+eraseLine {
		t.Fatalf("got %q", out.String())
	}
}

// Lines written to the wrapped log stream land above the live lines instead of tearing them.
func TestProgress_WrappedLogKeepsTheLiveLines(t *testing.T) {
	var out, log bytes.Buffer
	p := NewProgress(&out, terminalOf(200, 50), "")
	p.Add(textLine("n:1"))
	out.Reset()
	if _, err := p.Wrap(&log).Write([]byte("WARN x\n")); err != nil {
		t.Fatal(err)
	}
	if log.String() != "WARN x\n" || out.String() != "\r"+eraseLine+"\rn:1\n" {
		t.Fatalf("log %q, out %q", log.String(), out.String())
	}

	// Without a terminal the log is written as is.
	log.Reset()
	plain := NewProgress(&out, nil, "")
	if _, err := plain.Wrap(&log).Write([]byte("WARN y\n")); err != nil || log.String() != "WARN y\n" {
		t.Fatalf("log %q, err %v", log.String(), err)
	}
}
