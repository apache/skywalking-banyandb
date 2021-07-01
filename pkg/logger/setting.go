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
	"os"
	"strings"
	"sync"
	"time"

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
	var w io.Writer
	switch cfg.Env {
	case "dev":
		cw := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		cw.FormatLevel = func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
		}
		cw.FormatMessage = func(i interface{}) string {
			return fmt.Sprintf("***%s****", i)
		}
		cw.FormatFieldName = func(i interface{}) string {
			return fmt.Sprintf("%s:", i)
		}
		cw.FormatFieldValue = func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("%s", i))
		}
		w = io.Writer(cw)
	default:
		w = os.Stderr
	}
	l := zerolog.New(w).Level(lvl).With().Timestamp().Logger()
	return &Logger{module: "root", Logger: &l}, nil
}
