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

package logger

import (
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// recordingSink stands in for the real buffer. It records what was admitted so
// a test can assert on the split between the two destinations.
type recordingSink struct {
	mu      sync.Mutex
	entries []sinkEntry
}

type sinkEntry struct {
	module string
	line   string
	level  zerolog.Level
}

func (s *recordingSink) Admit(level zerolog.Level, module string, line []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, sinkEntry{level: level, module: module, line: string(line)})
}

func (s *recordingSink) levels() []zerolog.Level {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]zerolog.Level, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e.level)
	}
	return out
}

// withNative installs a sink and a native configuration for one test, and puts
// the package back the way it found it afterwards.
func withNative(t *testing.T, cfg Logging, native NativeLogging) *recordingSink {
	t.Helper()
	sink := &recordingSink{}
	SetNativeSink(sink)
	if err := InitWithNative(cfg, native); err != nil {
		t.Fatalf("InitWithNative: %v", err)
	}
	t.Cleanup(func() {
		StopNative()
		if err := InitWithNative(Logging{Env: "prod", Level: "info"}, NativeLogging{}); err != nil {
			t.Fatalf("restoring the logger: %v", err)
		}
	})
	return sink
}

// TestNativeDisabledAdmitsNothing is the guarantee that matters most to every
// existing deployment: with the feature off, no sink is ever reached, even when
// one is installed.
func TestNativeDisabledAdmitsNothing(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "debug"}, NativeLogging{Enabled: false})

	l := GetLogger("measure")
	l.Debug().Msg("debug")
	l.Error().Msg("error")

	if got := len(sink.levels()); got != 0 {
		t.Fatalf("sink received %d entries with native disabled, want 0", got)
	}
	if NativeEnabled() {
		t.Fatal("NativeEnabled reports true with native disabled")
	}
}

// TestTwoGatesSplitByLevel is the level model: normal logging quiet, native
// verbose, and an event admitted for one is not forced on the other.
func TestTwoGatesSplitByLevel(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "info"})

	l := GetLogger("measure")
	l.Debug().Msg("debug")
	l.Info().Msg("info")
	l.Warn().Msg("warn")
	l.Error().Msg("error")

	want := []zerolog.Level{zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel}
	got := sink.levels()
	if len(got) != len(want) {
		t.Fatalf("native received %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("native received %v, want %v", got, want)
		}
	}
}

// TestAdmissionFloorIsTheMoreVerboseSink pins the reason the root level cannot
// simply be the console level: zerolog filters once, upstream of both writers.
func TestAdmissionFloorIsTheMoreVerboseSink(t *testing.T) {
	for _, tc := range []struct {
		name     string
		console  zerolog.Level
		native   zerolog.Level
		nativeOn bool
		want     zerolog.Level
	}{
		{"native quieter", zerolog.InfoLevel, zerolog.WarnLevel, true, zerolog.InfoLevel},
		{"native louder", zerolog.ErrorLevel, zerolog.InfoLevel, true, zerolog.InfoLevel},
		{"native off", zerolog.ErrorLevel, zerolog.DebugLevel, false, zerolog.ErrorLevel},
		{"equal", zerolog.WarnLevel, zerolog.WarnLevel, true, zerolog.WarnLevel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := admissionFloor(tc.console, tc.native, tc.nativeOn); got != tc.want {
				t.Fatalf("admissionFloor = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestExcludedModulesNeverReachTheSink covers the feedback loop: the modules on
// the write path the sink publishes through must not be admitted, or one stored
// line produces the next.
func TestExcludedModulesNeverReachTheSink(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug"})

	GetLogger("stream").Info().Msg("on the write path")
	GetLogger("queue-client").Info().Msg("also on the write path")
	GetLogger("measure").Info().Msg("not on the write path")

	entries := sink.entries
	if len(entries) != 1 {
		t.Fatalf("sink received %d entries, want 1", len(entries))
	}
	if entries[0].module != "MEASURE" {
		t.Fatalf("sink received module %q, want MEASURE", entries[0].module)
	}
}

// TestExclusionMatchesByPrefix pins why the list cannot be exact-match: a
// --logging-modules override truncates the module name that is stamped, so an
// exact list would be bypassed by an unrelated console-side override.
func TestExclusionMatchesByPrefix(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug"})

	GetLogger("server-queue-pub-data").Info().Msg("suffixed module")

	if got := len(sink.levels()); got != 0 {
		t.Fatalf("sink received %d entries for a prefixed module, want 0", got)
	}
}

// TestSuppressionReportsAFullWrite pins zerolog's contract: a short count is
// mapped to io.ErrShortWrite by the multi-writer, so a gate that suppresses
// must still report the whole line as written.
func TestSuppressionReportsAFullWrite(t *testing.T) {
	line := []byte(`{"level":"debug"}`)

	native := &nativeWriter{module: "MEASURE", level: zerolog.WarnLevel}
	n, err := native.WriteLevel(zerolog.DebugLevel, line)
	if n != len(line) || err != nil {
		t.Fatalf("suppressed native write = (%d, %v), want (%d, nil)", n, err, len(line))
	}

	excluded := &nativeWriter{module: "STREAM", level: zerolog.DebugLevel, excluded: true}
	if n, err = excluded.WriteLevel(zerolog.ErrorLevel, line); n != len(line) || err != nil {
		t.Fatalf("excluded native write = (%d, %v), want (%d, nil)", n, err, len(line))
	}

	var out strings.Builder
	console := &consoleWriter{out: &out, level: zerolog.ErrorLevel}
	if n, err = console.WriteLevel(zerolog.InfoLevel, line); n != len(line) || err != nil {
		t.Fatalf("suppressed console write = (%d, %v), want (%d, nil)", n, err, len(line))
	}
	if out.Len() != 0 {
		t.Fatalf("suppressed console write emitted %q", out.String())
	}
}

// TestNativeWriteSurvivesAMissingSink covers the window between Init and the
// sink being installed, and the one after shutdown detaches it.
func TestNativeWriteSurvivesAMissingSink(t *testing.T) {
	StopNative()
	w := &nativeWriter{module: "MEASURE", level: zerolog.DebugLevel}
	line := []byte(`{"level":"info"}`)
	if n, err := w.WriteLevel(zerolog.InfoLevel, line); n != len(line) || err != nil {
		t.Fatalf("write with no sink = (%d, %v), want (%d, nil)", n, err, len(line))
	}
}

// TestStopNativeHaltsAdmission pins the shutdown order: admission stops at a
// known instant, so a final drain publishes exactly what was admitted before it.
func TestStopNativeHaltsAdmission(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug"})

	l := GetLogger("measure")
	l.Info().Msg("before")
	StopNative()
	l.Info().Msg("after")

	entries := sink.entries
	if len(entries) != 1 {
		t.Fatalf("sink received %d entries, want 1", len(entries))
	}
	if !strings.Contains(entries[0].line, "before") {
		t.Fatalf("sink received %q, want the line admitted before StopNative", entries[0].line)
	}
}

// TestNativeLevelIsRejectedWhenUnusable pins that a bad native level is an
// error rather than a silent fallback, matching the console level's treatment.
func TestNativeLevelIsRejectedWhenUnusable(t *testing.T) {
	t.Cleanup(func() {
		StopNative()
		if err := InitWithNative(Logging{Env: "prod", Level: "info"}, NativeLogging{}); err != nil {
			t.Fatalf("restoring the logger: %v", err)
		}
	})
	err := InitWithNative(Logging{Env: "prod", Level: "info"},
		NativeLogging{Enabled: true, Level: "not-a-level"})
	if err == nil {
		t.Fatal("InitWithNative accepted an unusable native level")
	}
}
