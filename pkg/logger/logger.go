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
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

var (
	ContextKey           = contextKey{}
	ErrNoLoggerInContext = errors.New("no logger in context")
)

type contextKey struct{}

// Logging is the config info
type Logging struct {
	Env     string
	Level   string
	Modules []string
	Levels  []string
}

// Logger is wrapper for rs/zerolog logger with module, it is singleton.
type Logger struct {
	module string
	*zerolog.Logger
	modules map[string]zerolog.Level
}

func (l Logger) Module() string {
	return l.module
}

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

// Loggable indicates the implement supports logging
type Loggable interface {
	SetLogger(*Logger)
}

func Fetch(ctx context.Context, name string) *Logger {
	return FetchOrDefault(ctx, name, nil)
}

func FetchOrDefault(ctx context.Context, name string, defaultLogger *Logger) *Logger {
	parentLogger := ctx.Value(ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*Logger); ok {
			return pl.Named(name)
		}
	}
	if defaultLogger == nil {
		return GetLogger(name)
	}
	return defaultLogger
}
