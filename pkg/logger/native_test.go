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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
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
	detach := AttachNativeSink(sink)
	if native.Enabled && native.TTLDays == 0 {
		native.TTLDays = 7
	}
	cfg.Native = native
	if err := Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		detach()
		if err := Init(Logging{Env: "prod", Level: "info"}); err != nil {
			t.Fatalf("restoring the logger: %v", err)
		}
	})
	return sink
}

// captureStderr replaces os.Stderr with a pipe for one test, so logging
// initialized after it prints there. The returned function puts stderr back
// and returns everything printed; nothing printed after it is collected.
// pkg/test offers the same for other packages, but it imports this one.
func captureStderr(t *testing.T) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prev := os.Stderr
	os.Stderr = w
	var buf bytes.Buffer
	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(drained)
	}()
	var once sync.Once
	read := func() string {
		once.Do(func() {
			os.Stderr = prev
			_ = w.Close()
			<-drained
		})
		return buf.String()
	}
	t.Cleanup(func() { read() })
	return read
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
	if nativeEnabled() {
		t.Fatal("nativeEnabled reports true with native disabled")
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

// TestAdmissionFloorIsTheMoreVerboseSink pins the reason a logger's level
// cannot simply be the console level: zerolog filters once, upstream of the
// writer.
func TestAdmissionFloorIsTheMoreVerboseSink(t *testing.T) {
	for _, tc := range []struct {
		name    string
		console string
		native  NativeLogging
		want    zerolog.Level
	}{
		{"native quieter", "info", NativeLogging{Enabled: true, Level: "warn"}, zerolog.InfoLevel},
		{"native louder", "error", NativeLogging{Enabled: true, Level: "info"}, zerolog.InfoLevel},
		{"native off", "error", NativeLogging{Level: "debug"}, zerolog.ErrorLevel},
		{"equal", "warn", NativeLogging{Enabled: true, Level: "warn"}, zerolog.WarnLevel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withNative(t, Logging{Env: "prod", Level: tc.console}, tc.native)
			if got := GetLogger("measure").GetLevel(); got != tc.want {
				t.Fatalf("admission floor = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestOrdinaryStorageLogsAreCollected keeps the protection precise: the
// stream and storage modules serve every group, so excluding them by name
// would discard what they log about ordinary writes.
func TestOrdinaryStorageLogsAreCollected(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug"})

	GetLogger("stream").Info().Msg("an ordinary write")
	GetLogger("storage").Info().Msg("an ordinary flush")

	for _, module := range []string{"STREAM", "STORAGE"} {
		if !sink.admittedFrom(module, zerolog.InfoLevel) {
			t.Errorf("%s was not admitted; only logging caused by native writes is excluded", module)
		}
	}
}

// TestReservedProtectionsSurviveAnOperatorList is the falsifying assertion for
// the recursion protections: an operator's list adds to them and can never
// remove one.
func TestReservedProtectionsSurviveAnOperatorList(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug", ExcludeModules: []string{"trace"}})

	GetLogger(NativeWriteModule).Info().Msg("the native log service")
	GetLogger(NativeWriteModule, "stream").Info().Msg("a native write on the write path")
	GetLogger("_monitoring_log", "shard0").Info().Msg("the log group's own storage")
	GetLogger("trace").Info().Msg("excluded by the operator")
	GetLogger("measure").Info().Msg("an ordinary module")

	for _, module := range []string{"NATIVE-LOG", "NATIVE-LOG.STREAM", "_MONITORING_LOG.SHARD0", "TRACE"} {
		if sink.admittedFrom(module, zerolog.InfoLevel) {
			t.Errorf("%s was admitted", module)
		}
	}
	if !sink.admittedFrom("MEASURE", zerolog.InfoLevel) {
		t.Fatal("MEASURE was not admitted, so the operator list excluded more than it named")
	}
}

// TestNativeWriteMarkReachesOnlyTheConsole is the falsifying assertion for the
// marker. The same call site logs through a context: under a native write the
// line is printed and not stored, and under any other write it is stored as
// usual. Nothing is switched off for the whole process.
func TestNativeWriteMarkReachesOnlyTheConsole(t *testing.T) {
	console := captureStderr(t)
	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{Enabled: true, Level: "info"})
	fallback := GetLogger("stream")

	FetchOrDefault(WithNativeWrite(context.Background()), "stream", fallback).Error().Msg("native-write-failed")
	FetchOrDefault(context.Background(), "stream", fallback).Error().Msg("user-write-failed")

	if !strings.Contains(console(), "native-write-failed") {
		t.Fatal("the line about a native write did not reach the console")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	var native, user bool
	for _, e := range sink.entries {
		native = native || strings.Contains(e.line, "native-write-failed")
		user = user || strings.Contains(e.line, "user-write-failed")
	}
	if native {
		t.Fatal("the line about a native write was stored natively, so storing it would log it again")
	}
	if !user {
		t.Fatal("the line about an ordinary write was not stored; the mark leaked past its context")
	}
}

// TestExclusionCoversAModuleAndItsChildren pins how an exclusion matches: a
// module and every module named under it, never a module that only shares the
// leading characters.
func TestExclusionCoversAModuleAndItsChildren(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug", ExcludeModules: []string{"measure"}})

	GetLogger("measure", "block").Info().Msg("a child of an excluded module")
	GetLogger("measurement").Info().Msg("a module that only shares the leading characters")

	if sink.admittedFrom("MEASURE.BLOCK", zerolog.InfoLevel) {
		t.Fatal("MEASURE.BLOCK was admitted although MEASURE is excluded")
	}
	if !sink.admittedFrom("MEASUREMENT", zerolog.InfoLevel) {
		t.Fatal("MEASUREMENT was not admitted; the exclusion of MEASURE matched it by its first characters")
	}
}

// TestSuppressionReportsAFullWrite pins zerolog's contract: a short count is
// mapped to io.ErrShortWrite, so a gate that suppresses must still report the
// whole line as written.
func TestSuppressionReportsAFullWrite(t *testing.T) {
	withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "warn", ExcludeModules: []string{"stream"}})
	line := []byte(`{"level":"debug"}`)

	var out strings.Builder
	quiet := &moduleWriter{console: &out, module: "MEASURE", consoleLevel: zerolog.ErrorLevel}
	n, err := quiet.WriteLevel(zerolog.DebugLevel, line)
	if n != len(line) || err != nil {
		t.Fatalf("suppressed write = (%d, %v), want (%d, nil)", n, err, len(line))
	}
	if out.Len() != 0 {
		t.Fatalf("suppressed console write emitted %q", out.String())
	}

	excluded := &moduleWriter{console: &out, module: "STREAM", consoleLevel: zerolog.ErrorLevel}
	if n, err = excluded.WriteLevel(zerolog.InfoLevel, line); n != len(line) || err != nil {
		t.Fatalf("excluded write = (%d, %v), want (%d, nil)", n, err, len(line))
	}
}

// TestNativeWriteSurvivesAMissingSink covers the window between Init and the
// sink being attached, and the one after shutdown detaches it.
func TestNativeWriteSurvivesAMissingSink(t *testing.T) {
	withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{Enabled: true, Level: "debug"})
	nativeSink.Store(nil)
	w := &moduleWriter{console: io.Discard, module: "MEASURE", consoleLevel: zerolog.ErrorLevel}
	line := []byte(`{"level":"info"}`)
	if n, err := w.WriteLevel(zerolog.InfoLevel, line); n != len(line) || err != nil {
		t.Fatalf("write with no sink = (%d, %v), want (%d, nil)", n, err, len(line))
	}
}

// TestDetachHaltsAdmission pins the shutdown order: admission stops at a known
// instant, so a final drain publishes exactly what was admitted before it.
func TestDetachHaltsAdmission(t *testing.T) {
	withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "debug"})
	sink := &recordingSink{}
	detach := AttachNativeSink(sink)

	l := GetLogger("measure")
	l.Info().Msg("before")
	detach()
	l.Info().Msg("after")

	entries := sink.entries
	if len(entries) != 1 {
		t.Fatalf("sink received %d entries, want 1", len(entries))
	}
	if !strings.Contains(entries[0].line, "before") {
		t.Fatalf("sink received %q, want the line admitted before detach", entries[0].line)
	}
}

// TestDetachLeavesALaterSinkAttached keeps a stale detach from cutting off the
// sink that replaced it.
func TestDetachLeavesALaterSinkAttached(t *testing.T) {
	first := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{Enabled: true, Level: "info"})
	_ = first
	stale := AttachNativeSink(&recordingSink{})
	later := &recordingSink{}
	defer AttachNativeSink(later)()
	stale()

	GetLogger("measure").Info().Msg("after a stale detach")
	if !later.admittedFrom("MEASURE", zerolog.InfoLevel) {
		t.Fatal("a stale detach removed the sink attached after it")
	}
}

// TestNativeLevelIsRejectedWhenUnusable pins that a bad native level is an
// error rather than a silent fallback, matching the console level's treatment.
func TestNativeLevelIsRejectedWhenUnusable(t *testing.T) {
	t.Cleanup(func() {
		if err := Init(Logging{Env: "prod", Level: "info"}); err != nil {
			t.Fatalf("restoring the logger: %v", err)
		}
	})
	err := Init(Logging{Env: "prod", Level: "info", Native: NativeLogging{Enabled: true, Level: "not-a-level"}})
	if err == nil {
		t.Fatal("Init accepted an unusable native level")
	}
}

// TestConsoleGateIsNotTheAdmissionFloor pins the independence of the two
// thresholds. The floor is the more verbose of the pair, so a console gate that
// reused it would print everything the native sink admitted and the quiet
// console the operator asked for would be silently undone.
func TestConsoleGateIsNotTheAdmissionFloor(t *testing.T) {
	console := captureStderr(t)

	withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "info"})

	l := GetLogger("measure")
	l.Info().Msg("info-line")
	l.Warn().Msg("warn-line")
	l.Error().Msg("error-line")

	out := console()
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
	captureStderr(t)
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

// TestNativeOverrideCannotLiftAnExclusion pins the safety rule: an override
// cannot re-admit an excluded module, reserved or configured.
func TestNativeOverrideCannotLiftAnExclusion(t *testing.T) {
	captureStderr(t)
	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{
		Enabled: true, Level: "debug", ExcludeModules: []string{"stream"},
		Modules: []string{"stream", NativeWriteModule}, Levels: []string{"debug", "debug"},
	})

	GetLogger("stream").Error().Msg("from an excluded module")
	GetLogger(NativeWriteModule).Error().Msg("from a reserved module")

	if sink.admittedFrom("STREAM", zerolog.ErrorLevel) {
		t.Fatal("an override re-admitted STREAM, which the operator excluded")
	}
	if sink.admittedFrom("NATIVE-LOG", zerolog.ErrorLevel) {
		t.Fatal("an override re-admitted NATIVE-LOG, which is reserved to break the feedback loop")
	}
}

// TestApplyNativeRejectsBadValues checks the validation of each flag that is
// left. The base configuration is valid, and each error must name the flag it
// is about, so a case cannot pass by failing for some other reason.
func TestApplyNativeRejectsBadValues(t *testing.T) {
	base := NativeLogging{Enabled: true, Level: "info", TTLDays: 7}
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
			name: "zero ttl", flag: "logging-native-ttl-days",
			mutate: func(c *NativeLogging) { c.TTLDays = 0 },
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

// TestOnlyTheKeptFlagsAreRegistered pins the public surface: buffering,
// writing and memory are internal, so their flags must not come back.
func TestOnlyTheKeptFlagsAreRegistered(t *testing.T) {
	fs := pflag.NewFlagSet("native", pflag.ContinueOnError)
	RegisterNativeFlags(fs, &NativeLogging{})
	var got []string
	fs.VisitAll(func(f *pflag.Flag) { got = append(got, f.Name) })
	want := []string{
		"logging-native-enabled", "logging-native-exclude-modules", "logging-native-level",
		"logging-native-levels", "logging-native-modules", "logging-native-ttl-days",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("native flags = %v, want %v", got, want)
	}
}

// TestLoggerBuiltBeforeInitGetsNative is the falsifying assertion for the
// package-level loggers. A var built with GetLogger while its package loads
// exists before Init runs, and must still reach the sink after it, without
// any change at the call site.
func TestLoggerBuiltBeforeInitGetsNative(t *testing.T) {
	captureStderr(t)
	early := GetLogger("early")

	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{Enabled: true, Level: "info"})
	early.Info().Msg("from the early logger")
	if !sink.admittedFrom("EARLY", zerolog.InfoLevel) {
		t.Fatal("a logger built before Init did not reach the sink after it")
	}

	// A second Init changes the policy the writer has already resolved once.
	sink = withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "info", ExcludeModules: []string{"early"}})
	early.Info().Msg("after the module is excluded")
	if sink.admittedFrom("EARLY", zerolog.InfoLevel) {
		t.Fatal("the logger kept the policy of an earlier Init; EARLY is excluded now")
	}
}

// TestNativePolicyIgnoresConsoleOverrides is the falsifying assertion for
// resolving native policy on the complete module. A console override for
// MEASURE shortens the module a MEASURE.BLOCK logger prints; native policy
// that followed the shortened name would lose the child's own override.
func TestNativePolicyIgnoresConsoleOverrides(t *testing.T) {
	console := captureStderr(t)
	sink := withNative(t, Logging{Env: "prod", Level: "info", Modules: []string{"measure"}, Levels: []string{"error"}},
		NativeLogging{Enabled: true, Level: "info", Modules: []string{"measure.block"}, Levels: []string{"debug"}})

	GetLogger("measure", "block").Debug().Msg("debug-from-block")

	if !sink.admittedFrom("MEASURE.BLOCK", zerolog.DebugLevel) {
		t.Fatal("the native override for MEASURE.BLOCK did not apply under a console override for MEASURE")
	}
	if strings.Contains(console(), "debug-from-block") {
		t.Fatal("the console printed a debug line although MEASURE is at error there")
	}
}

// TestChildExclusionHoldsUnderAParentConsoleOverride covers the exclusion side
// of the same rule: excluding MEASURE.BLOCK must not depend on the console's
// configuration of MEASURE.
func TestChildExclusionHoldsUnderAParentConsoleOverride(t *testing.T) {
	captureStderr(t)
	sink := withNative(t, Logging{Env: "prod", Level: "info", Modules: []string{"measure"}, Levels: []string{"error"}},
		NativeLogging{Enabled: true, Level: "info", ExcludeModules: []string{"measure.block"}})

	GetLogger("measure", "block").Info().Msg("from the excluded child")
	GetLogger("measure", "part").Info().Msg("from its sibling")

	if sink.admittedFrom("MEASURE.BLOCK", zerolog.InfoLevel) {
		t.Fatal("MEASURE.BLOCK was admitted although it is excluded; the console override for MEASURE hid the exclusion")
	}
	if !sink.admittedFrom("MEASURE.PART", zerolog.InfoLevel) {
		t.Fatal("MEASURE.PART was not admitted; the exclusion of MEASURE.BLOCK spread to its parent")
	}
}

// TestConsoleConfigurationDoesNotChangeNativeCollection compares what native
// stores with and without console overrides: the set must be the same.
func TestConsoleConfigurationDoesNotChangeNativeCollection(t *testing.T) {
	captureStderr(t)
	native := NativeLogging{
		Enabled: true, Level: "info",
		Modules: []string{"measure.block"}, Levels: []string{"debug"}, ExcludeModules: []string{"measure.part"},
	}
	emit := func() {
		for _, scope := range [][]string{{"measure", "block"}, {"measure", "part"}, {"measure", "merger"}, {"trace"}} {
			l := GetLogger(scope...)
			l.Debug().Msg("d")
			l.Info().Msg("i")
		}
	}
	collected := func(console Logging) []string {
		sink := withNative(t, console, native)
		emit()
		sink.mu.Lock()
		defer sink.mu.Unlock()
		out := make([]string, 0, len(sink.entries))
		for _, e := range sink.entries {
			out = append(out, e.module+"/"+e.level.String())
		}
		return out
	}
	plain := collected(Logging{Env: "prod", Level: "info"})
	overridden := collected(Logging{
		Env: "prod", Level: "warn",
		Modules: []string{"measure", "trace"}, Levels: []string{"error", "debug"},
	})
	if strings.Join(plain, ",") != strings.Join(overridden, ",") {
		t.Fatalf("console overrides changed what native stores:\nwithout: %v\nwith:    %v", plain, overridden)
	}
	want := "MEASURE.BLOCK/debug,MEASURE.BLOCK/info,MEASURE.MERGER/info,TRACE/info"
	if got := strings.Join(plain, ","); got != want {
		t.Fatalf("native stored %s, want %s", got, want)
	}
}

// TestToZapConfigFollowsTheConsole keeps ToZapConfig as it was before native
// storage: the logger's own level is the admission floor, which native lowers.
func TestToZapConfigFollowsTheConsole(t *testing.T) {
	withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{Enabled: true, Level: "debug"})
	if got := GetLogger("measure").ToZapConfig().Level.Level().String(); got != "error" {
		t.Fatalf("zap level = %s, want error", got)
	}
}

// TestDefaultLevelSetsTheConsoleLevel keeps DefaultLevel as it was before
// native storage: it sets what the console prints for a module with no
// override, and leaves native policy alone.
func TestDefaultLevelSetsTheConsoleLevel(t *testing.T) {
	console := captureStderr(t)
	sink := withNative(t, Logging{Env: "prod", Level: "info"}, NativeLogging{Enabled: true, Level: "info"})

	l := GetLogger("etcd").DefaultLevel(zerolog.ErrorLevel)
	l.Info().Msg("info-line")
	l.Error().Msg("error-line")

	if strings.Contains(console(), "info-line") {
		t.Fatal("DefaultLevel(error) did not quiet the console")
	}
	if !strings.Contains(console(), "error-line") {
		t.Fatal("DefaultLevel(error) silenced the console entirely")
	}
	if !sink.admittedFrom("ETCD", zerolog.InfoLevel) {
		t.Fatal("DefaultLevel changed native collection; the info line was not stored")
	}
}

// TestBlankExcludeEntryIsIgnored pins the shape of the exclusion list: a blank
// entry names no module and excludes nothing.
func TestBlankExcludeEntryIsIgnored(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "info", ExcludeModules: []string{"stream", "  "}})

	GetLogger("measure").Info().Msg("not on the write path")
	GetLogger("stream").Info().Msg("on the write path")

	if !sink.admittedFrom("MEASURE", zerolog.InfoLevel) {
		t.Fatal("MEASURE was not admitted, so the blank entry excluded every module")
	}
	if sink.admittedFrom("STREAM", zerolog.InfoLevel) {
		t.Fatal("STREAM was admitted although the operator listed it")
	}
}

// TestSinkOwnGroupStaysExcluded is the falsifying assertion for the feedback
// loop. A tsdb logger is named after its group, so the storage of the log group
// logs under _MONITORING_LOG; admitting those lines lets one stored line
// produce the next.
func TestSinkOwnGroupStaysExcluded(t *testing.T) {
	sink := withNative(t, Logging{Env: "prod", Level: "error"},
		NativeLogging{Enabled: true, Level: "info", ExcludeModules: []string{"something-else"}})

	GetLogger("_monitoring_log").Info().Msg("the log group's own storage")
	GetLogger("_monitoring_log", "segid-20260923", "shard0").Info().Msg("and its shard")
	GetLogger("measure").Info().Msg("an ordinary module")

	if !sink.admittedFrom("MEASURE", zerolog.InfoLevel) {
		t.Fatal("MEASURE was not admitted, so the operator list was not applied as given")
	}
	for _, module := range []string{"_MONITORING_LOG", "_MONITORING_LOG.SEGID-20260923.SHARD0"} {
		if sink.admittedFrom(module, zerolog.InfoLevel) {
			t.Errorf("%s was admitted, so the log storage feeds itself", module)
		}
	}
}

// TestLevelLessEventObeysTheThreshold is the falsifying assertion for the
// gate. zerolog's io.Writer logs at NoLevel, which is numerically above every
// real level, so an unnormalized comparison admits such an event whatever
// --logging-native-level says. The path is live: the index library reports
// through a stdlib log.Logger writing into one of ours.
func TestLevelLessEventObeysTheThreshold(t *testing.T) {
	t.Run("refused below the threshold", func(t *testing.T) {
		sink := withNative(t, Logging{Env: "prod", Level: "error"},
			NativeLogging{Enabled: true, Level: "error"})
		fmt.Fprintln(GetLogger("measure"), "a line from a library")
		if sink.admittedFrom("MEASURE", zerolog.NoLevel) {
			t.Fatal("a level-less event was stored although the native level is error")
		}
	})
	t.Run("admitted at the threshold", func(t *testing.T) {
		sink := withNative(t, Logging{Env: "prod", Level: "error"},
			NativeLogging{Enabled: true, Level: "info"})
		fmt.Fprintln(GetLogger("measure"), "a line from a library")
		if !sink.admittedFrom("MEASURE", zerolog.NoLevel) {
			t.Fatal("a level-less event was not stored although the native level is info")
		}
	})
}

// TestMostSpecificNativeOverrideWins keeps a child override effective when its
// parent has one too.
func TestMostSpecificNativeOverrideWins(t *testing.T) {
	captureStderr(t)
	sink := withNative(t, Logging{Env: "prod", Level: "error"}, NativeLogging{
		Enabled: true, Level: "warn",
		Modules: []string{"measure", "measure.block"}, Levels: []string{"info", "debug"},
	})
	GetLogger("measure", "block").Debug().Msg("child")
	GetLogger("measure", "part").Debug().Msg("sibling")
	if !sink.admittedFrom("MEASURE.BLOCK", zerolog.DebugLevel) {
		t.Fatal("the override for MEASURE.BLOCK lost to the one for MEASURE")
	}
	if sink.admittedFrom("MEASURE.PART", zerolog.DebugLevel) {
		t.Fatal("MEASURE.PART was admitted at debug; only MEASURE.BLOCK has that override")
	}
}
