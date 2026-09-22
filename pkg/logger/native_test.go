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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// recordingSink stands in for the real buffer. It records what was admitted so
// a test can assert on the split between the two destinations.
type recordingSink struct {
	entries []sinkEntry
	mu      sync.Mutex
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
	if native.Enabled {
		// Fill in the timing and sizing the validator requires, so a test only
		// has to state the part it is about.
		if native.FlushInterval == 0 {
			native.FlushInterval = time.Second
		}
		if native.FlushSize == 0 {
			native.FlushSize = 100
		}
		if native.MaxBytes == 0 {
			native.MaxBytes = 1 << 20
		}
		if native.MaxEventBytes == 0 {
			native.MaxEventBytes = 64 << 10
		}
		if native.ShardNum == 0 {
			native.ShardNum = 2
		}
		if native.TTLDays == 0 {
			native.TTLDays = 7
		}
		if native.WriteTimeout == 0 {
			native.WriteTimeout = 5 * time.Second
		}
		if native.DrainTimeout == 0 {
			native.DrainTimeout = 5 * time.Second
		}
		if native.MemoryFraction == 0 {
			native.MemoryFraction = 0.02
		}
	}
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

// swapConsoleTarget redirects normal logging for one test.
func swapConsoleTarget(w io.Writer) func() {
	prev := testConsoleTarget
	testConsoleTarget = w
	return func() { testConsoleTarget = prev }
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

// TestConsoleGateIsNotTheAdmissionFloor pins the independence of the two
// thresholds. The floor is the more verbose of the pair, so a console gate that
// reused it would print everything the native sink admitted and the quiet
// console the operator asked for would be silently undone.
func TestConsoleGateIsNotTheAdmissionFloor(t *testing.T) {
	var console strings.Builder
	restore := swapConsoleTarget(&console)
	t.Cleanup(restore)

	withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "info"})

	l := GetLogger("measure")
	l.Info().Msg("info-line")
	l.Warn().Msg("warn-line")
	l.Error().Msg("error-line")

	out := console.String()
	for _, quiet := range []string{"info-line", "warn-line"} {
		if strings.Contains(out, quiet) {
			t.Fatalf("console printed %q at --logging-level=error; output was %q", quiet, out)
		}
	}
	if !strings.Contains(out, "error-line") {
		t.Fatalf("console did not print the error line; output was %q", out)
	}
}

// admittedFrom reports whether the sink received a line from module at level.
func (s *recordingSink) admittedFrom(module string, level zerolog.Level) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.module == module && e.level == level {
			return true
		}
	}
	return false
}

// TestNativeModuleOverrideAppliesToOneModule is the falsifying assertion for
// --logging-native-modules / --logging-native-levels. The root native level is
// warn, so a debug line reaches storage only through the override -- and only
// for the module the override names, including its dotted children.
func TestNativeModuleOverrideAppliesToOneModule(t *testing.T) {
	defer swapConsoleTarget(io.Discard)()
	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{
		Enabled: true, Level: "warn",
		Modules: []string{"measure"}, Levels: []string{"debug"},
	})

	GetLogger("measure", "block").Debug().Msg("from the overridden module")
	GetLogger("trace").Debug().Msg("from a module with no override")

	if !sink.admittedFrom("MEASURE.BLOCK", zerolog.DebugLevel) {
		t.Fatal("the override for MEASURE did not reach MEASURE.BLOCK; its debug line was not admitted")
	}
	if sink.admittedFrom("TRACE", zerolog.DebugLevel) {
		t.Fatal("a module without an override was admitted at debug; the override leaked to the root level")
	}
}

// TestNativeOverrideCannotLiftAnExclusion pins the safety rule: the excluded
// modules sit on the write path the sink publishes through, so an override
// that re-admitted one would let a stored line produce the next.
func TestNativeOverrideCannotLiftAnExclusion(t *testing.T) {
	defer swapConsoleTarget(io.Discard)()
	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{
		Enabled: true, Level: "debug",
		Modules: []string{"stream"}, Levels: []string{"debug"},
	})

	GetLogger("stream").Error().Msg("from an excluded module")

	if sink.admittedFrom("STREAM", zerolog.ErrorLevel) {
		t.Fatal("an override re-admitted STREAM, which is excluded to break the write-path feedback loop")
	}
}

// TestApplyNativeRejectsBadValues checks each new flag's validation. The base
// configuration is valid, and each error must name the flag it is about, so a
// case cannot pass by failing for some other reason.
func TestApplyNativeRejectsBadValues(t *testing.T) {
	base := NativeLogging{
		Enabled: true, Level: "info", FlushInterval: time.Second, FlushSize: 100,
		MaxBytes: 1 << 20, MaxEventBytes: 64 << 10, ShardNum: 2, TTLDays: 7,
		WriteTimeout: 5 * time.Second, DrainTimeout: 5 * time.Second,
		MemoryFraction: 0.02, MemoryReserve: 64 << 20,
	}
	prev := nativeConfig.Load()
	t.Cleanup(func() { nativeConfig.Store(prev) })
	if err := applyNative(base); err != nil {
		t.Fatalf("the base configuration is rejected, so no case below can prove anything: %v", err)
	}

	tests := []struct {
		mutate func(*NativeLogging)
		name   string
		flag   string
	}{
		{
			name: "modules without levels", flag: "logging-native-modules",
			mutate: func(c *NativeLogging) { c.Modules = []string{"measure"} },
		},
		{
			name: "unknown level", flag: "native level",
			mutate: func(c *NativeLogging) { c.Modules = []string{"measure"}; c.Levels = []string{"loud"} },
		},
		{
			name: "zero fraction", flag: "logging-native-memory-fraction",
			mutate: func(c *NativeLogging) { c.MemoryFraction = 0 },
		},
		{
			name: "fraction above one", flag: "logging-native-memory-fraction",
			mutate: func(c *NativeLogging) { c.MemoryFraction = 1.5 },
		},
		{
			name: "negative reserve", flag: "logging-native-memory-reserve",
			mutate: func(c *NativeLogging) { c.MemoryReserve = -1 },
		},
		{
			name: "zero write timeout", flag: "logging-native-write-timeout",
			mutate: func(c *NativeLogging) { c.WriteTimeout = 0 },
		},
		{
			name: "zero drain timeout", flag: "logging-native-drain-timeout",
			mutate: func(c *NativeLogging) { c.DrainTimeout = 0 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := applyNative(cfg)
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tt.flag) {
				t.Fatalf("rejected for the wrong reason: %v", err)
			}
		})
	}
}

// TestLazyLoggerGetsNativeAfterInit is the falsifying assertion for the early
// loggers. Both loggers are created before InitWithNative, as a package-level
// var is. The one from GetLogger keeps the writers it was built with and never
// reaches the sink; the Lazy one, although used once before initialization,
// rebuilds after it and does.
func TestLazyLoggerGetsNativeAfterInit(t *testing.T) {
	defer swapConsoleTarget(io.Discard)()
	early := GetLogger("early")
	lazy := NewLazy("early")
	_ = lazy.Get()

	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{Enabled: true, Level: "info"})
	early.Info().Msg("from the eager logger")
	lazy.Get().Info().Msg("from the lazy logger")

	var eager, lazyLine bool
	sink.mu.Lock()
	for _, e := range sink.entries {
		eager = eager || strings.Contains(e.line, "from the eager logger")
		lazyLine = lazyLine || strings.Contains(e.line, "from the lazy logger")
	}
	sink.mu.Unlock()
	if !lazyLine {
		t.Fatal("the Lazy logger used before InitWithNative did not reach the sink after it")
	}
	if eager {
		t.Fatal("a logger built before InitWithNative reached the sink; the test no longer shows the problem it guards")
	}
}
