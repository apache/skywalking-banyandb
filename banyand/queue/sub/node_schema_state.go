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

package sub

import (
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// ruleTableChunkSize bounds how many index rules travel in one SchemaRuleTable
// event. A group can bind hundreds of rules whose full bodies would overflow a
// single gRPC message, so the table is split across events; the agent
// concatenates them in arrival order, which keeps bound_index_rule_refs valid.
const ruleTableChunkSize = 50

// StreamGroupSchemaState streams this node's cached and materialized schema
// bodies for a group (or every cached group when the request group is empty).
// The FODC agent fingerprints them on receive, so the data path ships no
// fingerprints. Each group's objects are followed by a SchemaSnapshotTrailer;
// collection errors surface on the trailer rather than failing the stream so a
// partial snapshot is still useful.
func (s *server) StreamGroupSchemaState(
	req *databasev1.NodeSchemaStateRequest,
	stream grpclib.ServerStreamingServer[databasev1.SchemaSnapshotEvent],
) error {
	if s.metadataRepo == nil {
		return status.Error(codes.Unavailable, "metadata repository not available")
	}
	node := s.curNode.GetMetadata().GetName()
	groups := []string{req.GetGroup()}
	if req.GetGroup() == "" {
		groups = s.metadataRepo.AllCachedGroups()
	}
	ctx := stream.Context()
	for _, group := range groups {
		if group == "" {
			continue
		}
		objects, ruleTable, found, err := s.metadataRepo.CollectGroupSchemaSnapshot(ctx, group)
		trailer := &databasev1.SchemaSnapshotTrailer{Group: group, Node: node}
		switch {
		case err != nil:
			trailer.Errors = []string{err.Error()}
		case found:
			for start := 0; start < len(ruleTable); start += ruleTableChunkSize {
				end := start + ruleTableChunkSize
				if end > len(ruleTable) {
					end = len(ruleTable)
				}
				if sendErr := stream.Send(&databasev1.SchemaSnapshotEvent{
					Event: &databasev1.SchemaSnapshotEvent_RuleTable{
						RuleTable: &databasev1.SchemaRuleTable{Group: group, Rules: ruleTable[start:end]},
					},
				}); sendErr != nil {
					return sendErr
				}
			}
			for _, obj := range objects {
				if sendErr := stream.Send(&databasev1.SchemaSnapshotEvent{
					Event: &databasev1.SchemaSnapshotEvent_Object{Object: obj},
				}); sendErr != nil {
					return sendErr
				}
			}
			trailer.ObjectCount = uint32(len(objects))
		}
		if sendErr := stream.Send(&databasev1.SchemaSnapshotEvent{
			Event: &databasev1.SchemaSnapshotEvent_Trailer{Trailer: trailer},
		}); sendErr != nil {
			return sendErr
		}
	}
	return nil
}
