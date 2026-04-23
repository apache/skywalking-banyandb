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

const (
	defaultCrashOutputDir = "crash"
	defaultMaxArtifacts   = 10
	defaultGoMemLimitPct  = 50
)

var (
	setCrashOutput = debug.SetCrashOutput
	// globalCrashFile retains the *os.File returned by os.OpenFile so that its
	// finalizer does not close the original fd while the process is running.
	// debug.SetCrashOutput internally duplicates the file descriptor, so the
	// runtime's crash-output fd is independent; however keeping this reference
	// also enables us to close the previous file cleanly on reinstall.
	globalCrashFile *os.File
	globalCrashPath string
)

// CrashOutputConfig controls whether runtime crash output is persisted to disk.
type CrashOutputConfig struct {
	Dir           string
	Enabled       bool
	MaxArtifacts  int
	GoMemLimitPct int
}

// NewCrashOutputConfig returns the default global crash-output configuration.
// Crash output is enabled by default; it is the final safety net for unrecoverable
// runtime failures and should always be active in production.
func NewCrashOutputConfig() CrashOutputConfig {
	return CrashOutputConfig{
		Enabled:       true,
		Dir:           defaultCrashOutputDir,
		MaxArtifacts:  defaultMaxArtifacts,
		GoMemLimitPct: defaultGoMemLimitPct,
	}
}

// RegisterFlags registers the crash-output flags on the provided flag set.
func (c *CrashOutputConfig) RegisterFlags(flags *pflag.FlagSet) {
	flags.BoolVar(&c.Enabled, "panic-diagnostics-enabled", c.Enabled,
		"enable runtime crash output persistence for fatal panics")
	flags.StringVar(&c.Dir, "panic-diagnostics-dir", c.Dir,
		"directory used to store panic diagnostics and runtime crash output")
	flags.IntVar(&c.MaxArtifacts, "panic-diagnostics-max-artifacts", c.MaxArtifacts,
		"maximum number of crash artifact directories to retain; oldest are removed first (0 disables pruning)")
	flags.IntVar(&c.GoMemLimitPct, "max-diagnosis-memory-usage-percentage", c.GoMemLimitPct,
		"set GOMEMLIMIT to this percentage of the cgroup memory limit, reserving headroom for post-panic diagnostics (0 disables)")
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

	// Close the previous file (safe because SetCrashOutput already dup'd its fd)
	// and store the new one so its finalizer doesn't run close() on us.
	if globalCrashFile != nil {
		_ = globalCrashFile.Close()
	}
	globalCrashFile = crashFile
	globalCrashPath = filePath

	SetDefaultMaxArtifacts(c.MaxArtifacts)

	if c.GoMemLimitPct > 0 {
		if _, memLimitErr := ApplyGoMemLimit(c.GoMemLimitPct); memLimitErr != nil {
			return fmt.Errorf("apply GOMEMLIMIT: %w", memLimitErr)
		}
	}

	return nil
}

func runtimeCrashFileName() string {
	return fmt.Sprintf("runtime-crash-%d.txt", os.Getpid())
}

// CleanupGlobalCrashOutput removes the current runtime crash output file when it
// exists but remained empty for the lifetime of the process.
func CleanupGlobalCrashOutput() error {
	if globalCrashFile == nil {
		return nil
	}

	crashFile := globalCrashFile
	crashPath := globalCrashPath
	globalCrashFile = nil
	globalCrashPath = ""

	if closeErr := crashFile.Close(); closeErr != nil {
		return fmt.Errorf("close runtime crash output file: %w", closeErr)
	}
	if crashPath == "" {
		return nil
	}

	info, statErr := os.Stat(crashPath)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return nil
		}
		return fmt.Errorf("stat runtime crash output file: %w", statErr)
	}
	if info.IsDir() || info.Size() > 0 {
		return nil
	}
	if removeErr := os.Remove(crashPath); removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("remove empty runtime crash output file: %w", removeErr)
	}
	return nil
}
