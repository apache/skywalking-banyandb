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
	"strings"

	"github.com/rs/zerolog"
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
	modules map[string]zerolog.Level
	module  string
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
	return &Logger{module: module, modules: l.modules, Logger: &subLogger}
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
