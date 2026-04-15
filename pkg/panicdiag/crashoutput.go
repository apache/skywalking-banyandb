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

package panicdiag

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/spf13/pflag"
)

const defaultCrashOutputDir = "crash"

var (
	setCrashOutput = debug.SetCrashOutput
	// globalCrashFile holds the crash output file open for the entire process lifetime.
	// debug.SetCrashOutput requires the underlying fd to remain valid until the process exits.
	globalCrashFile *os.File
)

// CrashOutputConfig controls whether runtime crash output is persisted to disk.
type CrashOutputConfig struct {
	Enabled bool
	Dir     string
}

// NewCrashOutputConfig returns the default global crash-output configuration.
func NewCrashOutputConfig() CrashOutputConfig {
	return CrashOutputConfig{
		Dir: defaultCrashOutputDir,
	}
}

// RegisterFlags registers the crash-output flags on the provided flag set.
func (c *CrashOutputConfig) RegisterFlags(flags *pflag.FlagSet) {
	flags.BoolVar(&c.Enabled, "panic-diagnostics-enabled", c.Enabled,
		"enable runtime crash output persistence for fatal panics")
	flags.StringVar(&c.Dir, "panic-diagnostics-dir", c.Dir,
		"directory used to store panic diagnostics and runtime crash output")
}

// InstallGlobalCrashOutput registers runtime/debug crash output when enabled.
func (c CrashOutputConfig) InstallGlobalCrashOutput() error {
	if !c.Enabled {
		return nil
	}
	if c.Dir == "" {
		return fmt.Errorf("panic diagnostics dir is empty")
	}

	if err := os.MkdirAll(c.Dir, 0o755); err != nil {
		return fmt.Errorf("create panic diagnostics dir: %w", err)
	}
	SetDefaultArtifactRoot(c.Dir)

	filePath := filepath.Join(c.Dir, runtimeCrashFileName())
	crashFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open runtime crash output file: %w", err)
	}

	if setCrashErr := setCrashOutput(crashFile, debug.CrashOptions{}); setCrashErr != nil {
		_ = crashFile.Close()
		return fmt.Errorf("set runtime crash output: %w", setCrashErr)
	}

	// Keep the file open for the entire process lifetime.
	// Closing it would invalidate the fd that debug.SetCrashOutput holds,
	// silently discarding any fatal crash output.
	if globalCrashFile != nil {
		_ = globalCrashFile.Close()
	}
	globalCrashFile = crashFile
	return nil
}

func runtimeCrashFileName() string {
	return fmt.Sprintf("runtime-crash-%d.txt", os.Getpid())
}
