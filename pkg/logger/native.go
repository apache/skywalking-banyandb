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
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
)

// NativeSink receives the events admitted for native self-storage. It is
// implemented outside this package, by the service that owns the buffer and the
// write path, and installed with SetNativeSink before Init runs.
//
// Admit is called on the goroutine that emitted the log line. It must never
// block, never panic and never report an error the caller has to handle: the
// logging path cannot be allowed to fail a caller that only wanted to log.
type NativeSink interface {
	Admit(level zerolog.Level, module string, line []byte)
}

// NativeLogging is the configuration of the native sink. It lives in the
// --logging-native-* namespace, which inherits nothing from --logging-*.
type NativeLogging struct {
	Level          string
	ExcludeModules []string
	// Modules and Levels override the native level per module, pairwise, the
	// way --logging-modules and --logging-levels do for the console.
	Modules       []string
	Levels        []string
	FlushInterval time.Duration
	// WriteTimeout bounds one batch publish; DrainTimeout bounds the final
	// drain at shutdown. The last publish of a drain may start just before the
	// drain deadline, so shutdown can take up to their sum.
	WriteTimeout time.Duration
	DrainTimeout time.Duration
	// MemoryFraction and MemoryReserve shape the adaptive budget where a memory
	// protector runs: min(MaxBytes, MemoryFraction * (available - MemoryReserve)).
	MemoryFraction float64
	MemoryReserve  int64
	MaxBytes       int64
	MaxEventBytes  int64
	FlushSize      int
	ShardNum       uint32
	TTLDays        uint32
	Enabled        bool
}

// defaultExcludedModules are never sent to the native sink. Every one of them
// sits on the write path the sink itself publishes through, so admitting them
// would let one stored line produce the next.
//
// Matching is by prefix, because a --logging-modules override truncates the
// module name it stamps: an exact-match list would be bypassed by an unrelated
// override on the console side.
var defaultExcludedModules = []string{
	"STREAM",
	"STORAGE",
	"QUEUE-CLIENT",
	"SERVER-QUEUE-SUB",
	"SERVER-QUEUE-PUB",
	"CLUSTER-NODE-REGISTRY",
	"LIAISON-GRPC",
	"WQUEUE",
	"MEMORY-PROTECTOR",
	"GRPC-HELPER",
	"NATIVE-LOG",
}

// mandatoryExcludedModules can never be admitted, whatever an operator
// configures. A tsdb logger is named after its group, so the storage of the
// log group itself logs under _MONITORING_LOG: admitting those lines would let
// one stored line produce the next. An operator list replaces the defaults, so
// this one is checked separately rather than living among them.
var mandatoryExcludedModules = []string{"_MONITORING_LOG"}

var (
	nativeSink   atomic.Pointer[NativeSink]
	nativeConfig atomic.Pointer[nativeState]
)

// nativeState is the resolved native configuration, published once by Init.
type nativeState struct {
	// modules maps an upper-cased module prefix to its native level.
	modules  map[string]zerolog.Level
	excluded []string
	level    zerolog.Level
	enabled  bool
}

// SetNativeSink installs the sink. It is called before Init, so that the
// buffer exists for the lines emitted while the process is still starting.
func SetNativeSink(s NativeSink) {
	if s == nil {
		nativeSink.Store(nil)
		return
	}
	nativeSink.Store(&s)
}

// StopNative detaches the sink. Admission stops at this instant, which is what
// lets a shutdown drain publish exactly the set admitted before it.
func StopNative() {
	nativeSink.Store(nil)
}

// NativeEnabled reports whether native self-storage was turned on.
func NativeEnabled() bool {
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
		"module prefixes never sent to native storage; replaces the built-in set rather than adding to it")
	fs.StringSliceVar(&cfg.Modules, "logging-native-modules", nil,
		"the modules whose native level overrides --logging-native-level; an excluded module stays excluded")
	fs.StringSliceVar(&cfg.Levels, "logging-native-levels", nil,
		"the native level of each module in --logging-native-modules, one per module")
	fs.DurationVar(&cfg.FlushInterval, "logging-native-flush-interval", time.Second,
		"longest a buffered event waits before it is written")
	fs.DurationVar(&cfg.WriteTimeout, "logging-native-write-timeout", 5*time.Second,
		"time limit for one batch publish; a batch that exceeds it is dropped and counted")
	fs.DurationVar(&cfg.DrainTimeout, "logging-native-drain-timeout", 5*time.Second,
		"time limit for writing what is still buffered at shutdown; the last publish can add up to --logging-native-write-timeout")
	fs.Float64Var(&cfg.MemoryFraction, "logging-native-memory-fraction", 0.02,
		"fraction of available memory, after the reserve, that the buffer may use where a memory protector runs")
	fs.Int64Var(&cfg.MemoryReserve, "logging-native-memory-reserve", 64<<20,
		"bytes of available memory kept out of the buffer budget where a memory protector runs")
	fs.IntVar(&cfg.FlushSize, "logging-native-flush-size", 100,
		"buffered events that trigger a write ahead of the interval")
	fs.Int64Var(&cfg.MaxBytes, "logging-native-max-bytes", 32<<20,
		"configured cap on the buffer, in bytes")
	fs.Int64Var(&cfg.MaxEventBytes, "logging-native-max-event-bytes", 64<<10,
		"events larger than this are dropped whole rather than truncated")
	fs.Uint32Var(&cfg.ShardNum, "logging-native-shard-num", 2,
		"shards of the _monitoring_log group, used when the group is created; routing follows the group's own count")
	fs.Uint32Var(&cfg.TTLDays, "logging-native-ttl-days", 7,
		"retention of the _monitoring_log group in days, used when the group is created")
}

// applyNative resolves the native configuration and publishes it for Named to
// read. An unusable level is an error here, as it is for the console level.
func applyNative(cfg NativeLogging) error {
	if !cfg.Enabled {
		nativeConfig.Store(&nativeState{enabled: false})
		return nil
	}
	lvl, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return err
	}
	// Rejected here rather than at first use: a non-positive interval makes
	// time.NewTicker panic inside the consumer, which would leave the feature
	// enabled and the buffer permanently undrained.
	if cfg.FlushInterval <= 0 {
		return fmt.Errorf("logging-native-flush-interval must be positive, got %s", cfg.FlushInterval)
	}
	if cfg.FlushSize <= 0 {
		return fmt.Errorf("logging-native-flush-size must be positive, got %d", cfg.FlushSize)
	}
	if cfg.MaxBytes <= 0 {
		return fmt.Errorf("logging-native-max-bytes must be positive, got %d", cfg.MaxBytes)
	}
	if cfg.MaxEventBytes <= 0 {
		return fmt.Errorf("logging-native-max-event-bytes must be positive, got %d", cfg.MaxEventBytes)
	}
	if cfg.ShardNum == 0 {
		return fmt.Errorf("logging-native-shard-num must be positive")
	}
	if cfg.TTLDays == 0 {
		return fmt.Errorf("logging-native-ttl-days must be positive")
	}
	if cfg.WriteTimeout <= 0 {
		return fmt.Errorf("logging-native-write-timeout must be positive, got %s", cfg.WriteTimeout)
	}
	if cfg.DrainTimeout <= 0 {
		return fmt.Errorf("logging-native-drain-timeout must be positive, got %s", cfg.DrainTimeout)
	}
	// Zero would give the buffer no budget at all wherever a memory protector
	// runs, which reads as a working feature that drops every event.
	if cfg.MemoryFraction <= 0 || cfg.MemoryFraction > 1 {
		return fmt.Errorf("logging-native-memory-fraction must be in (0, 1], got %g", cfg.MemoryFraction)
	}
	if cfg.MemoryReserve < 0 {
		return fmt.Errorf("logging-native-memory-reserve must not be negative, got %d", cfg.MemoryReserve)
	}
	modules, err := parseModuleLevels(cfg.Modules, cfg.Levels)
	if err != nil {
		return err
	}
	excluded := defaultExcludedModules
	if len(cfg.ExcludeModules) > 0 {
		configured := make([]string, 0, len(cfg.ExcludeModules))
		for _, m := range cfg.ExcludeModules {
			name := strings.ToUpper(strings.TrimSpace(m))
			// A blank entry is a prefix of every module name, so keeping one
			// would exclude everything and read as a feature that is on and
			// stores nothing.
			if name == "" {
				continue
			}
			configured = append(configured, name)
		}
		if len(configured) > 0 {
			excluded = configured
		}
	}
	nativeConfig.Store(&nativeState{enabled: true, level: lvl, excluded: excluded, modules: modules})
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

// resolveNative returns the native threshold for a module and whether the
// module is excluded. It runs once per module, when the logger is built, so
// that no event has to be parsed to discover where it belongs.
//
// Exclusion is checked first and an override cannot lift it: the excluded
// modules are the ones on the write path the sink publishes through.
func resolveNative(module string) (level zerolog.Level, excluded bool) {
	st := nativeConfig.Load()
	if st == nil || !st.enabled {
		return zerolog.Disabled, true
	}
	for _, prefix := range mandatoryExcludedModules {
		if strings.HasPrefix(module, prefix) {
			return zerolog.Disabled, true
		}
	}
	for _, prefix := range st.excluded {
		if strings.HasPrefix(module, prefix) {
			return zerolog.Disabled, true
		}
	}
	return overrideLevel(st.modules, module, st.level), false
}

// overrideLevel looks a module up the way Named does for the console: it walks
// the dotted module from its first component outward and takes the first
// override that matches, so "MEASURE" also covers "MEASURE.BLOCK".
func overrideLevel(modules map[string]zerolog.Level, module string, fallback zerolog.Level) zerolog.Level {
	if len(modules) == 0 {
		return fallback
	}
	end := 0
	for end < len(module) {
		next := strings.IndexByte(module[end:], '.')
		if next < 0 {
			end = len(module)
		} else {
			end += next
		}
		if lvl, ok := modules[module[:end]]; ok {
			return lvl
		}
		end++
	}
	return fallback
}

// consoleWriter gates normal logging on its own threshold. Suppression reports
// a full write: zerolog's multi-writer maps a short count to io.ErrShortWrite,
// which would surface on every suppressed line.
type consoleWriter struct {
	out   io.Writer
	level zerolog.Level
}

func (w *consoleWriter) Write(p []byte) (int, error) {
	return w.out.Write(p)
}

func (w *consoleWriter) WriteLevel(l zerolog.Level, p []byte) (int, error) {
	if l < w.level {
		return len(p), nil
	}
	return w.out.Write(p)
}

// nativeWriter gates native storage on its own threshold and hands what it
// admits to the installed sink. It reports a full write in every case,
// including when no sink is installed, so that a native failure can never
// degrade normal logging.
type nativeWriter struct {
	module   string
	level    zerolog.Level
	excluded bool
}

func (w *nativeWriter) Write(p []byte) (int, error) {
	return w.WriteLevel(zerolog.NoLevel, p)
}

func (w *nativeWriter) WriteLevel(l zerolog.Level, p []byte) (int, error) {
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
	if w.excluded || gate < w.level {
		return len(p), nil
	}
	if s := nativeSink.Load(); s != nil {
		(*s).Admit(l, w.module, p)
	}
	return len(p), nil
}

// admissionFloor is the more verbose of the thresholds actually in use. zerolog
// filters once, upstream of every writer, so a floor set to the quieter sink
// would starve the more verbose one before either gate saw the event.
func admissionFloor(console, native zerolog.Level, nativeOn bool) zerolog.Level {
	if !nativeOn || native > console {
		return console
	}
	return native
}
