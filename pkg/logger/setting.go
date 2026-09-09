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
	"github.com/spf13/pflag"
)

const (
	rootName = "ROOT"

	// Environment variables bound to the --logging-env and --logging-level flags by
	// pkg/config. They are read directly here because the root logger has to produce
	// output before the command tree that owns those flags exists.
	envLoggingEnv   = "BYDB_LOGGING_ENV"
	envLoggingLevel = "BYDB_LOGGING_LEVEL"
)

var root = rootLogger{}

type rootLogger struct {
	l    *Logger
	m    sync.Mutex
	done uint32
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
		rl.l, err = getLogger(defaultLogging())
		if err != nil {
			panic(err)
		}
	}
}

// defaultLogging returns the configuration the root logger falls back to until Init runs.
func defaultLogging() Logging {
	return earlyLogging(os.Args[1:], os.Getenv)
}

// earlyLogging resolves the logging configuration from the sources available before the
// command tree exists, so that the lines emitted while it is built follow the level the
// operator asked for. Flags win over the environment, the order config.Load applies later.
// An unusable level is ignored rather than fatal here, because Init reports it with a
// proper error once it runs.
func earlyLogging(args []string, getenv func(string) string) Logging {
	cfg := Logging{Env: "prod", Level: "debug"}
	if env := getenv(envLoggingEnv); env != "" {
		cfg.Env = env
	}
	cfg.Level = acceptLevel(cfg.Level, getenv(envLoggingLevel))
	flagEnv, flagLevel := loggingFlagsFromArgs(args)
	if flagEnv != "" {
		cfg.Env = flagEnv
	}
	cfg.Level = acceptLevel(cfg.Level, flagLevel)
	return cfg
}

// acceptLevel returns candidate when zerolog can parse it, and current otherwise.
func acceptLevel(current, candidate string) string {
	if candidate == "" {
		return current
	}
	if _, err := zerolog.ParseLevel(candidate); err != nil {
		return current
	}
	return candidate
}

// loggingFlagsFromArgs reads the two logging flags out of a raw argument list. It reuses
// pflag so the values match what cobra resolves later, and tolerates everything else in
// the list: the flags of the real command are not declared here, and the subcommand name
// is just a positional argument.
func loggingFlagsFromArgs(args []string) (env, level string) {
	fs := pflag.NewFlagSet("early-logging", pflag.ContinueOnError)
	fs.ParseErrorsAllowlist.UnknownFlags = true
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	fs.StringVar(&env, "logging-env", "", "")
	fs.StringVar(&level, "logging-level", "", "")
	if err := fs.Parse(args); err != nil {
		// A malformed command line is the real parser's business to report.
		return env, level
	}
	return env, level
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

func (rl *rootLogger) get() *Logger {
	rl.m.Lock()
	defer rl.m.Unlock()
	return rl.l
}

// GetLogger return logger with a scope.
func GetLogger(scope ...string) *Logger {
	root.verify()
	if len(scope) < 1 {
		return root.l
	}
	l := root.get()
	for _, v := range scope {
		l = l.Named(v)
	}
	return l
}

// RegisterFlags registers the logging flags shared by every BanyanDB binary.
func RegisterFlags(fs *pflag.FlagSet, logging *Logging) {
	fs.StringVar(&logging.Env, "logging-env", "prod", "the logging environment")
	fs.StringVar(&logging.Level, "logging-level", "info", "the root level of logging")
	fs.StringSliceVar(&logging.Modules, "logging-modules", nil, "the modules whose logging level overrides the root one")
	fs.StringSliceVar(&logging.Levels, "logging-levels", nil, "the logging level of each module, one per module")
}

// Init initializes a rs/zerolog logger from user config.
func Init(cfg Logging) (err error) {
	switch cfg.Env {
	case "prob", "":
		os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "ERROR")
		os.Setenv("GRPC_GO_LOG_FORMATTER", "json")
	case "dev":
		os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "INFO")
	}
	return root.set(cfg)
}

// getLogger initializes a root logger.
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
	development := strings.EqualFold(cfg.Env, "DEV")

	if development {
		cw := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		cw.FormatLevel = func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
		}
		w = io.Writer(cw)
	} else {
		w = os.Stderr
	}
	ctx := zerolog.New(w).Level(lvl).With().Timestamp()
	if development {
		ctx = ctx.Stack().Caller()
	}
	l := ctx.Logger()
	return &Logger{module: rootName, Logger: &l, modules: modules, development: development}, nil
}
