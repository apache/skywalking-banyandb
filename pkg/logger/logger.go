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

	"github.com/rs/zerolog"
)

// Logging is the config info
type Logging struct {
	Env   string
	Level string
}

// Logger is wrapper for rs/zerolog logger with module, it is singleton.
type Logger struct {
	module string
	*zerolog.Logger
}

func (l *Logger) Named(name string) *Logger {
	module := strings.Join([]string{l.module, name}, ".")
	subLogger := root.Logger.With().Str("module", module).Logger()
	return &Logger{module: module, Logger: &subLogger}
}
