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
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
)

// NativeSink receives the events admitted for native self-storage. It is
// implemented outside this package, by the service that owns the buffer and the
// write path, and attached with AttachNativeSink.
//
// Admit is called on the goroutine that emitted the log line. It must never
// block, never panic and never report an error the caller has to handle: the
// logging path cannot be allowed to fail a caller that only wanted to log.
type NativeSink interface {
	Admit(level zerolog.Level, module string, line []byte)
}

// NativeLogging is the configuration of the native sink. It lives in the
// --logging-native-* namespace, which inherits nothing from --logging-*.
// Buffering, batching, write handling and memory bounds are not configured:
// the native log service keeps them internal.
type NativeLogging struct {
	Level          string
	ExcludeModules []string
	// Modules and Levels override the native level per module, pairwise, the
	// way --logging-modules and --logging-levels do for the console.
	Modules []string
	Levels  []string
	TTLDays uint32
	Enabled bool
}

// NativeWriteModule names the logger of the native log service, and every
// logger taken from a context marked by WithNativeWrite.
const NativeWriteModule = "native-log"

// NativeLogGroup is the group native log events are stored in.
const NativeLogGroup = "_monitoring_log"

// reservedExcludedModules are never sent to native storage, whatever an
// operator configures; --logging-native-exclude-modules adds to them. Each
// covers logging caused by storing native logs, so admitting it would let one
// stored line produce the next:
//
//   - NATIVE-LOG is the native log service, and the write path while it
//     handles a native write, through the context WithNativeWrite marks.
//   - _MONITORING_LOG is the storage of the log group itself: a tsdb logger
//     is named after its group, and its segments, shards, flushes and merges
//     log under it.
var reservedExcludedModules = []string{strings.ToUpper(NativeWriteModule), strings.ToUpper(NativeLogGroup)}

// WithNativeWrite marks ctx as carrying a native log write. Work done under
// it that takes its logger from the context with Fetch or FetchOrDefault logs
// as a child of NATIVE-LOG: the line reaches the console, and never native
// storage. Other writes in the process, and the lines they log, are not
// affected.
func WithNativeWrite(ctx context.Context) context.Context {
	return context.WithValue(ctx, ContextKey, GetLogger(NativeWriteModule))
}

var (
	nativeSink   atomic.Pointer[NativeSink]
	nativeConfig atomic.Pointer[nativeState]
)

// nativeState is the resolved native configuration. Each Init publishes a new
// one, and a writer that finds a different one than it resolved against
// resolves again, so a logger built before Init follows the configuration in
// force when it writes.
type nativeState struct {
	// modules maps an upper-cased module to its native level.
	modules  map[string]zerolog.Level
	excluded []string
	level    zerolog.Level
	enabled  bool
}

// nativePolicy is how one module is treated under one nativeState.
type nativePolicy struct {
	state    *nativeState
	level    zerolog.Level
	excluded bool
}

// AttachNativeSink installs s as the destination of native logging and returns
// the function that detaches it. Admission stops the instant detach returns,
// which is what lets a shutdown drain publish exactly the set admitted before
// it. Detach leaves alone a sink attached after s.
func AttachNativeSink(s NativeSink) (detach func()) {
	p := &s
	nativeSink.Store(p)
	return func() { nativeSink.CompareAndSwap(p, nil) }
}

// nativeEnabled reports whether native self-storage is turned on.
func nativeEnabled() bool {
	st := nativeConfig.Load()
	return st != nil && st.enabled
}

// RegisterNativeFlags registers the native logging flags. Only the binaries
// that can reach a storage engine call it: restore and migration run when the
// data tier is unavailable, so they never offer the flags at all.
func RegisterNativeFlags(fs *pflag.FlagSet, cfg *NativeLogging) {
	fs.BoolVar(&cfg.Enabled, "logging-native-enabled", false,
		"store this process's own logs in BanyanDB, in addition to normal logging")
	fs.StringVar(&cfg.Level, "logging-native-level", "info",
		"the minimum level reaching native storage, independent of --logging-level")
	fs.StringSliceVar(&cfg.ExcludeModules, "logging-native-exclude-modules", nil,
		"modules never sent to native storage, with the modules named under them; adds to the built-in protections")
	fs.StringSliceVar(&cfg.Modules, "logging-native-modules", nil,
		"the modules whose native level overrides --logging-native-level; an excluded module stays excluded")
	fs.StringSliceVar(&cfg.Levels, "logging-native-levels", nil,
		"the native level of each module in --logging-native-modules, one per module")
	fs.Uint32Var(&cfg.TTLDays, "logging-native-ttl-days", 7,
		"retention of the _monitoring_log group in days, used when the group is created")
}

// applyNative validates the native configuration, resolves it and publishes
// it for the writers to read. An unusable value is an error here, as it is for
// the console level.
func applyNative(cfg NativeLogging) error {
	st, err := newNativeState(cfg)
	if err != nil {
		return err
	}
	if st.enabled {
		if err = validateNative(cfg); err != nil {
			return err
		}
	}
	nativeConfig.Store(st)
	return nil
}

// newNativeState resolves what decides admission: whether native is on, its
// level, the module overrides and the exclusions.
func newNativeState(cfg NativeLogging) (*nativeState, error) {
	if !cfg.Enabled {
		return &nativeState{enabled: false}, nil
	}
	lvl, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	modules, err := parseModuleLevels(cfg.Modules, cfg.Levels)
	if err != nil {
		return nil, err
	}
	var excluded []string
	for _, m := range cfg.ExcludeModules {
		if name := strings.ToUpper(strings.TrimSpace(m)); name != "" {
			excluded = append(excluded, name)
		}
	}
	return &nativeState{enabled: true, level: lvl, excluded: excluded, modules: modules}, nil
}

// validateNative checks the values that do not decide admission.
func validateNative(cfg NativeLogging) error {
	if cfg.TTLDays == 0 {
		return fmt.Errorf("logging-native-ttl-days must be positive")
	}
	return nil
}

// parseModuleLevels pairs the native module overrides with their levels. It
// applies the same rules as the console overrides: the lists must be the same
// length, and each level must parse.
func parseModuleLevels(modules, levels []string) (map[string]zerolog.Level, error) {
	if len(modules) != len(levels) {
		return nil, fmt.Errorf("logging-native-modules %v don't match logging-native-levels %v", modules, levels)
	}
	if len(modules) == 0 {
		return nil, nil
	}
	out := make(map[string]zerolog.Level, len(modules))
	for i, m := range modules {
		lvl, err := zerolog.ParseLevel(levels[i])
		if err != nil {
			return nil, fmt.Errorf("unknown native level %q for module %s: %w", levels[i], m, err)
		}
		out[strings.ToUpper(strings.TrimSpace(m))] = lvl
	}
	return out, nil
}

// resolve returns how a module is treated. module is the complete identity
// of the logger, every component Named was given, so a console override that
// shortens the module it prints cannot change the native decision.
//
// Exclusion is checked first and an override cannot lift it.
func (st *nativeState) resolve(module string) *nativePolicy {
	if st == nil || !st.enabled {
		return &nativePolicy{state: st, level: zerolog.Disabled, excluded: true}
	}
	for _, m := range reservedExcludedModules {
		if withinModule(module, m) {
			return &nativePolicy{state: st, level: zerolog.Disabled, excluded: true}
		}
	}
	for _, m := range st.excluded {
		if withinModule(module, m) {
			return &nativePolicy{state: st, level: zerolog.Disabled, excluded: true}
		}
	}
	return &nativePolicy{state: st, level: overrideLevel(st.modules, module, st.level)}
}

// withinModule reports whether module is parent or one of its children, so
// "MEASURE" covers "MEASURE.BLOCK" and not "MEASUREMENT".
func withinModule(module, parent string) bool {
	return module == parent || (strings.HasPrefix(module, parent) && module[len(parent)] == '.')
}

// overrideLevel returns the level of the most specific override that covers
// module: "MEASURE.BLOCK" wins over "MEASURE" for a MEASURE.BLOCK logger, and
// "MEASURE" still covers "MEASURE.PART".
func overrideLevel(modules map[string]zerolog.Level, module string, fallback zerolog.Level) zerolog.Level {
	if len(modules) == 0 {
		return fallback
	}
	level := fallback
	end := 0
	for end < len(module) {
		next := strings.IndexByte(module[end:], '.')
		if next < 0 {
			end = len(module)
		} else {
			end += next
		}
		if lvl, ok := modules[module[:end]]; ok {
			level = lvl
		}
		end++
	}
	return level
}

// moduleWriter is the output of every logger. It gates normal logging on the
// console threshold of its logger, and native storage on the policy of its
// module, which it resolves against the configuration in force when it writes.
//
// Both report a full write when they suppress: zerolog maps a short count to
// io.ErrShortWrite. A native failure never degrades normal logging.
type moduleWriter struct {
	console      io.Writer
	policy       atomic.Pointer[nativePolicy]
	module       string
	consoleLevel zerolog.Level
}

func (w *moduleWriter) Write(p []byte) (int, error) {
	return w.WriteLevel(zerolog.NoLevel, p)
}

func (w *moduleWriter) WriteLevel(l zerolog.Level, p []byte) (int, error) {
	var err error
	if l >= w.consoleLevel {
		_, err = w.console.Write(p)
	}
	st := nativeConfig.Load()
	if st == nil || !st.enabled {
		return len(p), err
	}
	pol := w.policy.Load()
	if pol == nil || pol.state != st {
		pol = st.resolve(w.module)
		w.policy.Store(pol)
	}
	// An event logged without a level arrives as NoLevel, which outranks every
	// real one and would therefore ignore the threshold entirely. That path is
	// live: a stdlib log.Logger writing into one of ours goes through
	// zerolog's io.Writer, which logs at NoLevel, and that is how the index
	// library reports. Such an event carries no severity, so it is admitted as
	// an informational one, and stored under its own name.
	gate := l
	if gate == zerolog.NoLevel {
		gate = zerolog.InfoLevel
	}
	if pol.excluded || gate < pol.level {
		return len(p), err
	}
	if s := nativeSink.Load(); s != nil {
		(*s).Admit(l, w.module, p)
	}
	return len(p), err
}

// admissionFloor is the zerolog level of a logger: the more verbose of its
// console threshold and the native threshold of its module. zerolog filters
// once, upstream of the writer, so a floor set to the quieter destination would
// starve the more verbose one before either gate saw the event.
func admissionFloor(console zerolog.Level, module string) zerolog.Level {
	pol := nativeConfig.Load().resolve(module)
	if pol.excluded || pol.level > console {
		return console
	}
	return pol.level
}
