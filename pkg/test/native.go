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

package test

import (
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// NativeLines is a native sink that keeps every line native storage would
// receive.
type NativeLines struct {
	lines []string
	mu    sync.Mutex
}

// Admit keeps line.
func (n *NativeLines) Admit(_ zerolog.Level, _ string, line []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lines = append(n.lines, string(line))
}

// Count returns how many lines were kept.
func (n *NativeLines) Count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.lines)
}

// NativeLoggingAtWarn turns native logging on at warn and returns the sink
// that receives what is stored natively. Logging is initialized again when the
// test ends, so later tests do not inherit native logging.
func NativeLoggingAtWarn(t testing.TB) *NativeLines {
	t.Helper()
	t.Cleanup(func() { _ = logger.Init(logger.Logging{Env: "prod", Level: "info"}) })
	if err := logger.Init(logger.Logging{
		Env: "prod", Level: "warn",
		Native: logger.NativeLogging{Enabled: true, Level: "warn", TTLDays: 1},
	}); err != nil {
		t.Fatalf("cannot turn native logging on: %v", err)
	}
	sink := &NativeLines{}
	t.Cleanup(logger.AttachNativeSink(sink))
	return sink
}
