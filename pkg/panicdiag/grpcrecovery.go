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
	"runtime/debug"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// GRPCRecoveryHandler returns a handler for the gRPC recovery interceptors that reports
// the panic through panicdiag before converting it to a codes.Internal error. Servers
// previously hand-rolled a handler that only logged, which left every panic recovered on
// a request path out of banyandb_panic_total, out of the crash reporters and without an
// artifact -- the panics most likely to matter were the ones nothing counted.
//
// The returned function is deliberately untyped so this package does not depend on the
// gRPC middleware; it satisfies recovery.RecoveryHandlerFuncContext directly:
//
//	recovery.WithRecoveryHandlerContext(panicdiag.GRPCRecoveryHandler(log, "grpc.liaison"))
func GRPCRecoveryHandler(log *logger.Logger, component string) func(context.Context, any) error {
	return func(ctx context.Context, panicValue any) error {
		// Captured here, inside the interceptor's recovering defer, so the stack still
		// describes the panic site rather than the recovery machinery.
		RecoverExternal(ctx, RecoveryOptions{Logger: log, Component: component}, nil, panicValue, debug.Stack())
		return status.Errorf(codes.Internal, "%s", panicValue)
	}
}
