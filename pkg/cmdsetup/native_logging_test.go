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

package cmdsetup

import (
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestUnsupportedRoleKeepsItsLevel is the falsifying assertion for the flag's
// blast radius. InitWithNative lowers every logger's threshold to the native
// level, so a role that stores nothing must not enable it: the process would
// encode lines that the console gate and the empty sink both discard.
func TestUnsupportedRoleKeepsItsLevel(t *testing.T) {
	t.Cleanup(func() {
		logger.StopNative()
		if err := logger.InitWithNative(logger.Logging{Env: "prod", Level: "info"}, logger.NativeLogging{}); err != nil {
			t.Fatalf("restoring the logger: %v", err)
		}
	})
	for _, tt := range []struct {
		annotation string
		name       string
		want       bool
	}{
		{name: "liaison", annotation: "", want: false},
		{name: "data", annotation: "supported", want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: tt.name, Annotations: map[string]string{}}
			if tt.annotation != "" {
				cmd.Annotations[nativeLoggingAnnotation] = tt.annotation
			}
			if err := initNativeLogging(cmd,
				logger.Logging{Env: "prod", Level: "error"},
				logger.NativeLogging{
					Enabled: true, Level: "debug", MaxBytes: 1 << 20, MaxEventBytes: 64 << 10,
					FlushSize: 10, FlushInterval: time.Second, WriteTimeout: time.Second,
					DrainTimeout: time.Second, MemoryFraction: 0.1, ShardNum: 1, TTLDays: 1,
				}); err != nil {
				t.Fatalf("initNativeLogging: %v", err)
			}
			if got := logger.NativeEnabled(); got != tt.want {
				t.Errorf("native logging enabled = %v on the %s role, want %v", got, tt.name, tt.want)
			}
		})
	}
}
