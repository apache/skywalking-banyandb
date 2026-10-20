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

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	nativelog "github.com/apache/skywalking-banyandb/banyand/observability/logging"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestUnsupportedRoleKeepsItsLevel is the falsifying assertion for the flag's
// blast radius. Native lowers a logger's admission floor to the native level,
// so a role that stores nothing must not enable it: the process would encode
// lines that the console gate and the absent sink both discard.
func TestUnsupportedRoleKeepsItsLevel(t *testing.T) {
	native := logger.NativeLogging{Enabled: true, Level: "debug", TTLDays: 1}
	nativeLogSink = nativelog.NewSink()
	t.Cleanup(func() {
		nativeLogSink.Detach()
		if err := logger.Init(logger.Logging{Env: "prod", Level: "info"}); err != nil {
			t.Fatalf("restoring the logger: %v", err)
		}
	})
	for _, tt := range []struct {
		annotation string
		name       string
		want       zerolog.Level
	}{
		{name: "liaison", annotation: "", want: zerolog.ErrorLevel},
		{name: "data", annotation: "supported", want: zerolog.DebugLevel},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: tt.name, Annotations: map[string]string{}}
			if tt.annotation != "" {
				cmd.Annotations[nativeLoggingAnnotation] = tt.annotation
			}
			if err := initLogging(cmd, logger.Logging{Env: "prod", Level: "error", Native: native}); err != nil {
				t.Fatalf("initLogging: %v", err)
			}
			if got := logger.GetLogger("measure").GetLevel(); got != tt.want {
				t.Errorf("a logger on the %s role admits from %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
