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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logging is the config info
type Logging struct {
	Env   string
	Level string
}

// Logger is wrapper for zap logger with module, it is singleton.
type Logger struct {
	module string
	*zap.Logger
}

func (l *Logger) Named(name string) *Logger {
	module := strings.Join([]string{l.module, name}, ".")
	return &Logger{module: module, Logger: root.Logger.Named(module)}
}

// String constructs a field with the given key and value.
func String(key string, val string) zap.Field {
	return zap.Field{Key: key, Type: zapcore.StringType, String: val}
}

// Error is shorthand for the common idiom NamedError("error", err).
func Error(err error) zap.Field {
	return zap.NamedError("error", err)
}

// Uint32 constructs a field with the given key and value.
func Uint32(key string, val uint32) zap.Field {
	return zap.Field{Key: key, Type: zapcore.Uint32Type, Integer: int64(val)}
}

// Uint64 constructs a field with the given key and value.
func Uint64(key string, val uint64) zap.Field {
	return zap.Field{Key: key, Type: zapcore.Uint64Type, Integer: int64(val)}
}

// Int32 constructs a field with the given key and value.
func Int32(key string, val int32) zap.Field {
	return zap.Field{Key: key, Type: zapcore.Int32Type, Integer: int64(val)}
}

// Int64 constructs a field with the given key and value.
func Int64(key string, val int64) zap.Field {
	return zap.Field{Key: key, Type: zapcore.Int64Type, Integer: val}
}

// Binary constructs a field with the given key and value.
func Binary(key string, value interface{}) zap.Field {
	return zap.Field{Key: key, Type: zapcore.BinaryType, Interface: value}
}

// Any takes a key and an arbitrary value and chooses the best way to represent
// them as a field, falling back to a reflection-based approach only if
// necessary.
func Any(key string, value interface{}) zap.Field {
	return zap.Any(key, value)
}
