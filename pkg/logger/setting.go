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
	"os"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

var (
	root *Logger
	once sync.Once
)

// GetLogger return logger with a scope
func GetLogger(scope ...string) *Logger {
	if len(scope) < 1 {
		return root
	}
	module := strings.Join(scope, ".")
	subLogger := root.Logger.With().Str("module", module).Logger()
	return &Logger{module: module, Logger: &subLogger}
}

// Bootstrap logging for system boot
func Bootstrap() (err error) {
	once.Do(func() {
		root, err = getLogger(Logging{
			Env:   "dev",
			Level: "debug",
		})
	})
	if err != nil {
		return err
	}
	return nil
}

// Init initializes a rs/zerolog logger from user config
func Init(cfg Logging) (err error) {
	once.Do(func() {
		root, err = getLogger(cfg)
	})
	if err != nil {
		return err
	}
	return nil
}

// getLogger initializes a root logger
func getLogger(cfg Logging) (*Logger, error) {
	lvl, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	w := io.Writer(os.Stderr)
	switch cfg.Env {
	case "dev":
		w = zerolog.ConsoleWriter{Out: os.Stderr}
	default:
	}
	l := zerolog.New(w).Level(lvl).With().Timestamp().Logger()
	return &Logger{module: "root", Logger: &l}, nil
}
