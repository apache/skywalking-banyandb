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
// Panics are intercepted, the panic value and localized stack trace are logged,
// and banyandb_panic_total is incremented with component as the label.
// This is the project-wide entry point for launching background goroutines,
// following TiDB's high-reliability goroutine pattern.
func Go(ctx context.Context, component string, log *logger.Logger, fn func(context.Context)) {
	panicdiag.GoWithRecovery(ctx, panicdiag.RecoveryOptions{
		Component: component,
		Logger:    log,
	}, nil, func(ctxPtr *context.Context) {
		fn(*ctxPtr)
	})
}
