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

package protector

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Nop is a no-op implementation of Memory interface for testing.
type Nop struct{}

// Ensure Nop implements Memory interface.
var _ Memory = (*Nop)(nil)

// ShouldCache always returns false.
func (Nop) ShouldCache(int64) bool { return false }

// AvailableBytes always returns -1 (unlimited).
func (Nop) AvailableBytes() int64 { return -1 }

// GetLimit always returns 0 (no limit).
func (Nop) GetLimit() uint64 { return 0 }

// AcquireResource always succeeds.
func (Nop) AcquireResource(_ context.Context, _ uint64) error { return nil }

// Name returns the protector name.
func (Nop) Name() string { return "nop-protector" }

// FlagSet returns an empty flag set.
func (Nop) FlagSet() *run.FlagSet { return run.NewFlagSet("nop") }

// Validate always succeeds.
func (Nop) Validate() error { return nil }

// PreRun does nothing.
func (Nop) PreRun(context.Context) error { return nil }

// Serve returns a closed channel.
func (Nop) Serve() run.StopNotify {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// GracefulStop does nothing.
func (Nop) GracefulStop() {}
