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
}

// Logger is wrapper for rs/zerolog logger with module, it is singleton.
type Logger struct {
	*zerolog.Logger
	modules     map[string]zerolog.Level
	module      string
	development bool
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
	level := l.GetLevel()
	for i, m := range mm {
		if i != 0 {
			moduleBuilder.WriteString(".")
		}
		moduleBuilder.WriteString(strings.ToUpper(m))
		module = moduleBuilder.String()
		if ml, ok := l.modules[module]; ok {
			level = ml
		}
	}
	subLogger := root.l.With().Str("module", moduleBuilder.String()).Logger().Level(level)
	return &Logger{module: module, modules: l.modules, development: l.development, Logger: &subLogger}
}

// Sampled return a Logger with a sampler that will send every Nth events.
func (l *Logger) Sampled(n uint32) *Logger {
	sampled := l.Logger.Sample(&zerolog.BasicSampler{N: n})
	l.Logger = &sampled
	return l
}

// ToZapConfig outputs the zap config is derived from l.
func (l *Logger) ToZapConfig() zap.Config {
	level, err := zap.ParseAtomicLevel(l.GetLevel().String())
	if err != nil {
		panic(err)
	}
	if !l.development {
		config := zap.NewProductionConfig()
		config.Level = level
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
