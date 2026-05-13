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

	grpclib "google.golang.org/grpc"
)

// BreadcrumbUnaryInterceptor returns a gRPC unary server interceptor that pre-installs
// a mutable breadcrumb store into the request context. Place this BEFORE the recovery
// interceptor so that breadcrumbs appended inside the handler are visible to the
// recovery handler's defer, which captures the context at interceptor entry.
func BreadcrumbUnaryInterceptor() grpclib.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpclib.UnaryServerInfo, handler grpclib.UnaryHandler) (any, error) {
		return handler(WithMutableBreadcrumbs(ctx), req)
	}
}

// BreadcrumbStreamInterceptor returns a gRPC stream server interceptor that pre-installs
// a mutable breadcrumb store. It wraps the ServerStream so that stream.Context() returns
// the enriched context — this is required because the recovery interceptor calls
// stream.Context() in its defer rather than capturing a ctx variable.
func BreadcrumbStreamInterceptor() grpclib.StreamServerInterceptor {
	return func(srv any, ss grpclib.ServerStream, _ *grpclib.StreamServerInfo, handler grpclib.StreamHandler) error {
		return handler(srv, &breadcrumbWrappedStream{
			ServerStream: ss,
			ctx:          WithMutableBreadcrumbs(ss.Context()),
		})
	}
}

type breadcrumbWrappedStream struct {
	grpclib.ServerStream
	ctx context.Context
}

// Context returns the enriched context that contains the mutable breadcrumb store.
func (w *breadcrumbWrappedStream) Context() context.Context {
	return w.ctx
}
