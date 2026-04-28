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

package run

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

// Go launches fn in a new goroutine protected by panic recovery.
// A mutable breadcrumb store is installed before fn runs so breadcrumbs added
// inside fn are captured if fn panics.
// Panics are intercepted, and the panic value and localized stack trace are logged.
// When a panic counter is configured via RecoveryOptions.Counter or
// panicdiag.SetDefaultPanicCounter, banyandb_panic_total is incremented with
// component as the label.
func Go(ctx context.Context, component string, log *logger.Logger, fn func(context.Context)) {
	panicdiag.GoWithRecovery(ctx, panicdiag.RecoveryOptions{
		Component: component,
		Logger:    log,
	}, nil, func(ctxPtr *context.Context) {
		// Install a mutable breadcrumb store so breadcrumbs added inside
		// fn are visible to the recovery defer (which reads *ctxPtr after
		// fn returns or panics).
		*ctxPtr = panicdiag.WithMutableBreadcrumbs(*ctxPtr)
		fn(*ctxPtr)
	})
}
