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

// Package logger implements a logging system with a module tag.
// The module tag represents a scope where the log event is emitted.
package logger

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ContextKey is the key to store Logger in the context.
var ContextKey = contextKey{}

type contextKey struct{}

// Logging is the config info.
type Logging struct {
	Env     string
	Level   string
	Modules []string
	Levels  []string
	// Native configures the native destination, which stores this process's
	// own logs in BanyanDB. It is off unless Native.Enabled is set.
	Native NativeLogging
}

// Logger is wrapper for rs/zerolog logger with module, it is singleton.
type Logger struct {
	*zerolog.Logger
	modules map[string]zerolog.Level
	// consoleTarget is where normal logging writes. Named needs it to build a
	// gate per module rather than sharing the root's writer.
	consoleTarget io.Writer
	module        string
	// identity is every component the logger was named with. module stops at
	// the component a console override matched; native policy is resolved
	// against identity, so the console overrides cannot change it.
	identity       string
	consoleLevel   zerolog.Level
	development    bool
	isDefaultLevel bool
}

// Module returns logger's module name.
func (l Logger) Module() string {
	return l.module
}

// Named creates a new Logger and assigns a module to it.
func (l *Logger) Named(name ...string) *Logger {
	var mm []string
	if l.module == rootName {
		mm = name
	} else {
		mm = append([]string{l.module}, name...)
	}
	var moduleBuilder strings.Builder
	var module string
	// The console threshold, not the logger's own level: with native enabled
	// that level is the admission floor -- the more verbose of the two
	// destinations -- and reusing it here would print what only native wants.
	level := l.consoleLevel
	isDefaultLevel := true
	for i, m := range mm {
		if i != 0 {
			moduleBuilder.WriteString(".")
		}
		moduleBuilder.WriteString(strings.ToUpper(m))
		module = moduleBuilder.String()
		if ml, ok := l.modules[module]; ok {
			level = ml
			isDefaultLevel = false
			break
		}
	}
	identity := strings.ToUpper(strings.Join(name, "."))
	if l.identity != "" {
		identity = l.identity + "." + identity
	}
	parent := root.get()
	child := &Logger{
		module: module, identity: identity, modules: l.modules, development: l.development,
		isDefaultLevel: isDefaultLevel, consoleTarget: parent.consoleTarget, consoleLevel: level,
	}
	subLogger := parent.Logger.Output(child.writer()).With().Str("module", moduleBuilder.String()).Logger().
		Level(admissionFloor(level, child.nativeModule()))
	child.Logger = &subLogger
	return child
}

// nativeModule is the module native storage knows this logger by.
func (l *Logger) nativeModule() string {
	if l.identity == "" {
		return rootName
	}
	return l.identity
}

// writer builds the output of l. It has no state but its gates, so a logger
// built before Init writes under the native configuration Init installs.
func (l *Logger) writer() *moduleWriter {
	return &moduleWriter{console: l.consoleTarget, module: l.nativeModule(), consoleLevel: l.consoleLevel}
}

// ToZapConfig outputs the zap config is derived from l.
//
// The console threshold is what it follows, as before native storage existed:
// the logger's own level is the admission floor, which native may lower.
func (l *Logger) ToZapConfig() zap.Config {
	level, err := zap.ParseAtomicLevel(l.consoleLevel.String())
	if err != nil {
		panic(err)
	}
	if !l.development {
		config := zap.NewProductionConfig()
		switch l.consoleLevel {
		case zerolog.DebugLevel:
			config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		case zerolog.InfoLevel:
			config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		case zerolog.WarnLevel:
			config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
		default:
			config.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
		}
		return config
	}
	encoderConfig := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:       "T",
		LevelKey:      "L",
		NameKey:       "N",
		CallerKey:     "C",
		FunctionKey:   zapcore.OmitKey,
		MessageKey:    "M",
		StacktraceKey: "S",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel: func(l zapcore.Level, pae zapcore.PrimitiveArrayEncoder) {
			pae.AppendString(strings.ToUpper(fmt.Sprintf("| %-6s|", l.String())))
		},
		EncodeTime: func(t time.Time, pae zapcore.PrimitiveArrayEncoder) {
			pae.AppendString(t.Format(time.RFC3339))
		},
		EncodeDuration:   zapcore.StringDurationEncoder,
		EncodeCaller:     zapcore.FullCallerEncoder,
		ConsoleSeparator: " ",
	}
	return zap.Config{
		Level:            level,
		Development:      true,
		Encoding:         "console",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}

// DefaultLevel sets the default level for the logger. It is the console
// threshold it sets; the native threshold of the module is left as configured.
func (l *Logger) DefaultLevel(level zerolog.Level) *Logger {
	if l.isDefaultLevel {
		l.consoleLevel = level
		nl := l.Logger.Output(l.writer()).Level(admissionFloor(level, l.nativeModule()))
		l.Logger = &nl
	}
	return l
}

// Loggable indicates the implement supports logging.
type Loggable interface {
	SetLogger(*Logger)
}

// Fetch gets a Logger in a context, then creates a new Logger based on it.
func Fetch(ctx context.Context, newModuleName string) *Logger {
	return FetchOrDefault(ctx, newModuleName, nil)
}

// FetchOrDefault gets a Logger in a context, then creates a new Logger based on it
// If the context doesn't include a Logger. The default Logger will be picked.
func FetchOrDefault(ctx context.Context, newModuleName string, defaultLogger *Logger) *Logger {
	parentLogger := ctx.Value(ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*Logger); ok {
			return pl.Named(newModuleName)
		}
	}
	if defaultLogger == nil {
		return GetLogger(newModuleName)
	}
	return defaultLogger
}
