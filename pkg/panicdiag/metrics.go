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
	defaultAbortFuncPtr    atomic.Pointer[AbortFunc]
)

// SetDefaultPanicCounter registers a process-wide panic counter used by WithRecovery
// when RecoveryOptions.Counter is nil. Call once during process initialization.
func SetDefaultPanicCounter(counter meter.Counter) {
	if counter == nil {
		return
	}
	defaultPanicCounterPtr.Store(&counter)
}

// SetDefaultReporter registers a process-wide Reporter that WithRecovery
// invokes for every recovered panic, in addition to any per-call reporter
// supplied by the caller. Pass nil to clear a previously registered default.
// Call once during process initialization.
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

// callReporter invokes both the process-wide default reporter (if any) and the
// per-call reporter (if any) so that callers supplying their own reporter do
// not silently bypass the default, for example, the FODC agent's in-process
// panic store registered via SetDefaultReporter must observe every recovered
// panic regardless of whether a particular WithRecovery caller also passes a
// local reporter. The default fires first so its bookkeeping is unaffected by
// any error or panic in the per-call reporter.
func callReporter(ctx context.Context, reporter Reporter, result RecoveryResult) {
	if ptr := defaultReporterPtr.Load(); ptr != nil {
		(*ptr)(ctx, result)
	}
	if reporter != nil {
		reporter(ctx, result)
	}
}

// SetDefaultAbortFunc registers a process-wide AbortFunc that WithRecovery
// invokes for every recovered panic, in addition to any per-call OnAbort
// supplied via RecoveryOptions. Pass nil to clear a previously registered
// default. Call once during process initialization.
func SetDefaultAbortFunc(f AbortFunc) {
	if f == nil {
		defaultAbortFuncPtr.Store(nil)
		return
	}
	defaultAbortFuncPtr.Store(&f)
}

// callAbort invokes both the process-wide default abort hook (if any) and the
// per-call hook (if any). The default fires first so its bookkeeping is
// unaffected by anything the per-call hook does. Aborts run after every
// Reporter and before any Repanic, giving callers a single point to fail the
// parent: cancel a context, signal a lifecycle group, etc.
func callAbort(ctx context.Context, abort AbortFunc, result RecoveryResult) {
	if ptr := defaultAbortFuncPtr.Load(); ptr != nil {
		(*ptr)(ctx, result)
	}
	if abort != nil {
		abort(ctx, result)
	}
}
