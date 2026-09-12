// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package grpc

import (
	"context"

	"google.golang.org/grpc/stats"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

type queryAdmissionStatsHandler struct{}

func (queryAdmissionStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	scopedCtx, _ := query.NewTransportScope(ctx)
	return scopedCtx
}

func (queryAdmissionStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	if _, ok := rpcStats.(*stats.End); !ok {
		return
	}
	if scope, ok := query.TransportScopeFromContext(ctx); ok {
		scope.Close()
	}
}

func (queryAdmissionStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (queryAdmissionStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
