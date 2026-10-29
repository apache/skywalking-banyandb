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

package stream

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type nativeLines struct {
	lines []string
	mu    sync.Mutex
}

func (n *nativeLines) Admit(_ zerolog.Level, _ string, line []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lines = append(n.lines, string(line))
}

func (n *nativeLines) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.lines)
}

// TestWriteCallbackKeepsNativeWritesOffNativeStorage is the falsifying
// assertion for the feedback protection on the write path. The callback logs
// a refused write: for a native log write that line must reach the console
// and not native storage, where storing it would refuse and log again. The
// same refusal for any other write is stored as usual.
func TestWriteCallbackKeepsNativeWritesOffNativeStorage(t *testing.T) {
	printed := len(console.String())
	// Logging is initialized again afterwards, so later tests do not inherit
	// native logging.
	defer func() { _ = logger.Init(logger.Logging{Env: "prod", Level: "info"}) }()
	require.NoError(t, logger.Init(logger.Logging{
		Env: "prod", Level: "warn",
		Native: logger.NativeLogging{Enabled: true, Level: "warn", TTLDays: 1},
	}))
	sink := &nativeLines{}
	defer logger.AttachNativeSink(sink)()

	w := &writeCallback{l: logger.GetLogger("stream")}
	malformed := bus.NewMessage(1, "not a batch")

	w.Rev(logger.WithNativeWrite(context.Background()), malformed)
	require.Eventually(t, func() bool { return strings.Contains(console.String()[printed:], "invalid event data type") },
		5*time.Second, 10*time.Millisecond, "the refused native write was not reported on the console")
	if n := sink.count(); n != 0 {
		t.Fatalf("the refused native write was stored natively %d times", n)
	}

	w.Rev(context.Background(), malformed)
	if n := sink.count(); n != 1 {
		t.Fatalf("an ordinary refused write was stored %d times, want 1", n)
	}
}
