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

import "testing"

func TestEarlyLogging(t *testing.T) {
	tests := []struct {
		env       map[string]string
		name      string
		wantEnv   string
		wantLevel string
		args      []string
	}{
		{
			name:      "nothing set keeps the built-in defaults",
			wantEnv:   "prod",
			wantLevel: "debug",
		},
		{
			name:      "level from the environment",
			env:       map[string]string{envLoggingLevel: "error"},
			wantEnv:   "prod",
			wantLevel: "error",
		},
		{
			name:      "env from the environment",
			env:       map[string]string{envLoggingEnv: "dev"},
			wantEnv:   "dev",
			wantLevel: "debug",
		},
		{
			name:      "level from a flag joined by an equals sign",
			args:      []string{"liaison", "--logging-level=error"},
			wantEnv:   "prod",
			wantLevel: "error",
		},
		{
			name:      "level from a flag separated by a space",
			args:      []string{"liaison", "--logging-level", "error"},
			wantEnv:   "prod",
			wantLevel: "error",
		},
		{
			name:      "both flags at once",
			args:      []string{"data", "--logging-env", "dev", "--logging-level", "warn"},
			wantEnv:   "dev",
			wantLevel: "warn",
		},
		{
			name:      "a flag wins over the environment",
			env:       map[string]string{envLoggingLevel: "info", envLoggingEnv: "prod"},
			args:      []string{"standalone", "--logging-level=error", "--logging-env=dev"},
			wantEnv:   "dev",
			wantLevel: "error",
		},
		{
			name:      "the environment still applies to what the flags leave out",
			env:       map[string]string{envLoggingEnv: "dev"},
			args:      []string{"standalone", "--logging-level=error"},
			wantEnv:   "dev",
			wantLevel: "error",
		},
		{
			name:      "flags of the real command do not disturb the scan",
			args:      []string{"data", "--grpc-port", "17912", "--logging-level=error", "--node-labels", "type=hot"},
			wantEnv:   "prod",
			wantLevel: "error",
		},
		{
			name:      "an unparsable level from a flag falls back",
			args:      []string{"liaison", "--logging-level=bogus"},
			wantEnv:   "prod",
			wantLevel: "debug",
		},
		{
			name:      "an unparsable level from the environment falls back",
			env:       map[string]string{envLoggingLevel: "bogus"},
			wantEnv:   "prod",
			wantLevel: "debug",
		},
		{
			name:      "an unparsable flag does not discard a usable environment value",
			env:       map[string]string{envLoggingLevel: "error"},
			args:      []string{"liaison", "--logging-level=bogus"},
			wantEnv:   "prod",
			wantLevel: "error",
		},
		{
			name:      "a trailing flag with no value is tolerated",
			args:      []string{"liaison", "--logging-level"},
			wantEnv:   "prod",
			wantLevel: "debug",
		},
		{
			name:      "an explicitly empty env flag wins over the environment",
			env:       map[string]string{envLoggingEnv: "dev"},
			args:      []string{"liaison", "--logging-env="},
			wantEnv:   "",
			wantLevel: "debug",
		},
		{
			name:      "an explicitly empty level flag wins over the environment",
			env:       map[string]string{envLoggingLevel: "error"},
			args:      []string{"liaison", "--logging-level="},
			wantEnv:   "prod",
			wantLevel: "",
		},
		{
			name:      "an empty environment value does not override the default",
			env:       map[string]string{envLoggingEnv: "", envLoggingLevel: ""},
			wantEnv:   "prod",
			wantLevel: "debug",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(key string) string { return tt.env[key] }
			cfg := earlyLogging(tt.args, getenv)
			if cfg.Env != tt.wantEnv {
				t.Errorf("Env = %q, want %q", cfg.Env, tt.wantEnv)
			}
			if cfg.Level != tt.wantLevel {
				t.Errorf("Level = %q, want %q", cfg.Level, tt.wantLevel)
			}
		})
	}
}
