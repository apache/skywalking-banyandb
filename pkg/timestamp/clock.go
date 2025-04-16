// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package timestamp

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
)

// Clock represents an interface contains all functions in the standard library time.
type Clock interface {
	clock.Clock
}

// MockClock represents a mock clock that only moves forward programmatically.
type MockClock interface {
	clock.Clock
	// Add moves the current time of the mock clock forward by the specified duration.
	Add(d time.Duration)
	// Set sets the current time of the mock clock to a specific one.
	Set(t time.Time)
	// TriggerTimer sends the current time to timer.C
	TriggerTimer() bool
}

// NewClock returns an instance of a real-time clock.
func NewClock() Clock {
	return clock.New()
}

// NewMockClock returns an instance of a mock clock.
func NewMockClock() MockClock {
	return clock.NewMock()
}

var clockKey = contextClockKey{}

type contextClockKey struct{}

// GetClock returns a Clock from the context. If the context doesn't contains
// a Clock, a real-time one will be created and returned with a child context
// which contains the new one.
func GetClock(ctx context.Context) (Clock, context.Context) {
	c := ctx.Value(clockKey)
	if c == nil {
		realClock := NewClock()
		return realClock, SetClock(ctx, realClock)
	}
	return c.(Clock), ctx
}

// SetClock returns a sub context with the passed Clock.
func SetClock(ctx context.Context, clock Clock) context.Context {
	return context.WithValue(ctx, clockKey, clock)
}
