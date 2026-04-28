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
	"context"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

var (
	defaultPanicCounterPtr atomic.Pointer[meter.Counter]
	defaultReporterPtr     atomic.Pointer[Reporter]
)

// SetDefaultPanicCounter registers a process-wide panic counter used by WithRecovery
// when RecoveryOptions.Counter is nil. Call once during process initialization.
func SetDefaultPanicCounter(counter meter.Counter) {
	if counter == nil {
		return
	}
	defaultPanicCounterPtr.Store(&counter)
}

// SetDefaultReporter registers a process-wide Reporter used by WithRecovery when the
// caller passes nil as the reporter argument. Call once during process initialization.
func SetDefaultReporter(r Reporter) {
	if r == nil {
		defaultReporterPtr.Store(nil)
		return
	}
	defaultReporterPtr.Store(&r)
}

func incPanicCounter(counter meter.Counter, component string) {
	c := counter
	if c == nil {
		if ptr := defaultPanicCounterPtr.Load(); ptr != nil {
			c = *ptr
		}
	}
	if c == nil {
		return
	}
	c.Inc(1, component)
}

func callReporter(ctx context.Context, reporter Reporter, result RecoveryResult) {
	if reporter != nil {
		reporter(ctx, result)
		return
	}
	if ptr := defaultReporterPtr.Load(); ptr != nil {
		(*ptr)(ctx, result)
	}
}
