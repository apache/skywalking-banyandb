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
	FlushInterval  time.Duration
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

var (
	nativeSink   atomic.Pointer[NativeSink]
	nativeConfig atomic.Pointer[nativeState]
)

// nativeState is the resolved native configuration, published once by Init.
type nativeState struct {
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
	fs.DurationVar(&cfg.FlushInterval, "logging-native-flush-interval", time.Second,
		"longest a buffered event waits before it is written")
	fs.IntVar(&cfg.FlushSize, "logging-native-flush-size", 100,
		"buffered events that trigger a write ahead of the interval")
	fs.Int64Var(&cfg.MaxBytes, "logging-native-max-bytes", 32<<20,
		"configured cap on the buffer, in bytes")
	fs.Int64Var(&cfg.MaxEventBytes, "logging-native-max-event-bytes", 64<<10,
		"events larger than this are dropped whole rather than truncated")
	fs.Uint32Var(&cfg.ShardNum, "logging-native-shard-num", 2,
		"shards of the _monitoring_log group; raisable later through the group schema")
	fs.Uint32Var(&cfg.TTLDays, "logging-native-ttl-days", 7,
		"retention of the _monitoring_log group, in days")
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
	excluded := defaultExcludedModules
	if len(cfg.ExcludeModules) > 0 {
		excluded = make([]string, 0, len(cfg.ExcludeModules))
		for _, m := range cfg.ExcludeModules {
			excluded = append(excluded, strings.ToUpper(strings.TrimSpace(m)))
		}
	}
	nativeConfig.Store(&nativeState{enabled: true, level: lvl, excluded: excluded})
	return nil
}

// resolveNative returns the native threshold for a module and whether the
// module is excluded. It runs once per module, when the logger is built, so
// that no event has to be parsed to discover where it belongs.
func resolveNative(module string) (level zerolog.Level, excluded bool) {
	st := nativeConfig.Load()
	if st == nil || !st.enabled {
		return zerolog.Disabled, true
	}
	for _, prefix := range st.excluded {
		if strings.HasPrefix(module, prefix) {
			return zerolog.Disabled, true
		}
	}
	return st.level, false
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
	if w.excluded || l < w.level {
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
