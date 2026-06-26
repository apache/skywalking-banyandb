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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package lifecycle

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

func TestIsNoNodes(t *testing.T) {
	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "typed sentinel", err: node.ErrNoAvailableNode, want: true},
		{name: "wrapped sentinel", err: fmt.Errorf("pick failed: %w", node.ErrNoAvailableNode), want: true},
		{name: "plain round-robin string", err: errors.New("no nodes available"), want: true},
		{name: "wrapped round-robin string", err: fmt.Errorf("replica 0: %w", errors.New("no nodes available")), want: true},
		{name: "unrelated error", err: errors.New("connection refused"), want: false},
		{name: "nil error", err: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isNoNodes(tt.err))
		})
	}
}

func TestIsTransientSend(t *testing.T) {
	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "server busy sentinel", err: queue.ErrServerBusy, want: true},
		{name: "wrapped server busy", err: fmt.Errorf("receiver busy on node x: %w", queue.ErrServerBusy), want: true},
		{name: "plain non-transient", err: errors.New("permission denied"), want: false},
		{name: "nil error", err: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isTransientSend(tt.err))
		})
	}
}

// fakePicker implements nodePicker: it returns "no nodes available" the first
// noNodes times, then failErr (if set), then node.
type fakePicker struct {
	failErr error
	node    string
	noNodes int
	calls   int
}

func (f *fakePicker) Pick(_, _ string, _, _ uint32) (string, error) {
	f.calls++
	if f.noNodes > 0 {
		f.noNodes--
		return "", errors.New("no nodes available")
	}
	if f.failErr != nil {
		return "", f.failErr
	}
	return f.node, nil
}

func TestPickWithRetry(t *testing.T) {
	l := logger.GetLogger("test")

	t.Run("success first try", func(t *testing.T) {
		p := &fakePicker{node: "n1"}
		got, err := pickWithRetry(l, p, "g", "n", 0)
		assert.NoError(t, err)
		assert.Equal(t, "n1", got)
		assert.Equal(t, 1, p.calls)
	})

	t.Run("permanent error fails fast without retry", func(t *testing.T) {
		boom := errors.New("unknown shard")
		p := &fakePicker{failErr: boom}
		got, err := pickWithRetry(l, p, "g", "n", 0)
		assert.ErrorIs(t, err, boom)
		assert.Empty(t, got)
		assert.Equal(t, 1, p.calls)
	})

	t.Run("no nodes then recover retries", func(t *testing.T) {
		p := &fakePicker{node: "n2", noNodes: 1}
		got, err := pickWithRetry(l, p, "g", "n", 0)
		assert.NoError(t, err)
		assert.Equal(t, "n2", got)
		assert.Equal(t, 2, p.calls)
	})
}
