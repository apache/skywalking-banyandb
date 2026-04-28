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

	"github.com/spf13/pflag"
)

const (
	defaultCrashOutputDir = "crash"
	defaultMaxArtifacts   = 10
	defaultGoMemLimitPct  = 50
)

// CrashOutputConfig controls structured panic diagnostics.
type CrashOutputConfig struct {
	Dir           string
	Enabled       bool
	MaxArtifacts  int
	GoMemLimitPct int
}

// NewCrashOutputConfig returns the default global crash-output configuration.
// Structured panic diagnostics are enabled by default.
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
		"enable structured panic diagnostics")
	flags.StringVar(&c.Dir, "panic-diagnostics-dir", c.Dir,
		"directory used to store structured panic diagnostics")
	flags.IntVar(&c.MaxArtifacts, "panic-diagnostics-max-artifacts", c.MaxArtifacts,
		"maximum number of crash artifact directories to retain; oldest are removed first (0 disables pruning)")
	flags.IntVar(&c.GoMemLimitPct, "max-diagnosis-memory-usage-percentage", c.GoMemLimitPct,
		"set GOMEMLIMIT to this percentage of the cgroup memory limit, reserving headroom for post-panic diagnostics (0 disables)")
}

// InstallGlobalCrashOutput configures structured panic diagnostics when enabled.
func (c CrashOutputConfig) InstallGlobalCrashOutput() error {
	if !c.Enabled {
		return nil
	}
	if c.Dir == "" {
		return fmt.Errorf("panic diagnostics dir is empty")
	}

	SetDefaultArtifactRoot(c.Dir)
	SetDefaultMaxArtifacts(c.MaxArtifacts)

	if c.GoMemLimitPct > 0 {
		if _, memLimitErr := ApplyGoMemLimit(c.GoMemLimitPct); memLimitErr != nil {
			return fmt.Errorf("apply GOMEMLIMIT: %w", memLimitErr)
		}
	}

	return nil
}
