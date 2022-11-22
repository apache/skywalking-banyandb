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
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

const rootName = "ROOT"

var root = rootLogger{}

type rootLogger struct {
	done uint32
	m    sync.Mutex
	l    *Logger
}

func (rl *rootLogger) verify() {
	if atomic.LoadUint32(&root.done) == 0 {
		rl.setDefault()
	}
}

func (rl *rootLogger) setDefault() {
	rl.m.Lock()
	defer rl.m.Unlock()
	if rl.done == 0 {
		defer atomic.StoreUint32(&rl.done, 1)
		var err error
		rl.l, err = getLogger(Logging{
			Env:   "prod",
			Level: "debug",
		})
		if err != nil {
			panic(err)
		}
	}
}

func (rl *rootLogger) set(cfg Logging) error {
	rl.m.Lock()
	defer rl.m.Unlock()
	var err error
	rl.l, err = getLogger(cfg)
	if err != nil {
		return err
	}
	atomic.StoreUint32(&rl.done, 1)
	return nil
}

// GetLogger return logger with a scope
func GetLogger(scope ...string) *Logger {
	root.verify()
	if len(scope) < 1 {
		return root.l
	}
	l := root.l
	for _, v := range scope {
		l = l.Named(v)
	}
	return l
}

// Init initializes a rs/zerolog logger from user config
func Init(cfg Logging) (err error) {
	return root.set(cfg)
}

// getLogger initializes a root logger
func getLogger(cfg Logging) (*Logger, error) {
	modules := make(map[string]zerolog.Level)
	if len(cfg.Modules) > 0 {
		if len(cfg.Modules) != len(cfg.Levels) {
			return nil, fmt.Errorf("modules %v don't match levels %v", cfg.Modules, cfg.Levels)
		}
		for i, v := range cfg.Modules {
			lvl, err := zerolog.ParseLevel(cfg.Levels[i])
			if err != nil {
				return nil, errors.WithMessagef(err, "unknown module level %s", v)
			}
			modules[strings.ToUpper(v)] = lvl
		}
	}
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
		w = os.Stdout
	}
	l := zerolog.New(w).Level(lvl).With().Timestamp().Logger()
	return &Logger{module: rootName, Logger: &l, modules: modules}, nil
}
