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

// Package cluster connects the data transfer commands to a cluster through its liaisons: the
// connection preflight, the group listing and the dry-run Plan call of an export.
package cluster

import (
	"context"
	"slices"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// FetchGroups lists every group through the liaison's registry service but the groups the
// server creates for its own bookkeeping, which are no data to transfer.
func FetchGroups(ctx context.Context, conn *grpc.ClientConn) ([]*commonv1.Group, error) {
	resp, err := databasev1.NewGroupRegistryServiceClient(conn).List(ctx, &databasev1.GroupRegistryServiceListRequest{})
	if err != nil {
		return nil, exitcode.Exit(exitcode.For(err), "list groups: %w", err)
	}
	return slices.DeleteFunc(resp.GetGroup(), func(g *commonv1.Group) bool {
		return common.IsInternalGroup(g.GetMetadata().GetName())
	}), nil
}
