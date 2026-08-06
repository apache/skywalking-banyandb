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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// fakeSchemaStream captures the events StreamGroupSchemaState sends. Only Send
// and Context are exercised; the rest of the ServerStream surface is never
// called, so the embedded nil interface is fine.
type fakeSchemaStream struct {
	grpclib.ServerStream
	ctx    context.Context
	events []*databasev1.SchemaSnapshotEvent
}

func (f *fakeSchemaStream) Send(e *databasev1.SchemaSnapshotEvent) error {
	f.events = append(f.events, e)
	return nil
}

func (f *fakeSchemaStream) Context() context.Context { return f.ctx }

func TestStreamGroupSchemaState_SplitsRuleTableAndPreservesOrder(t *testing.T) {
	// 120 rules must span ceil(120/50)=3 SchemaRuleTable events, each capped at
	// the chunk size, and concatenate back in the original order so every
	// bound_index_rule_ref still resolves.
	const ruleCount = 120
	rules := make([]*databasev1.IndexRule, ruleCount)
	for i := range rules {
		rules[i] = &databasev1.IndexRule{Metadata: &commonv1.Metadata{Group: "g", Name: fmt.Sprintf("r%d", i)}}
	}
	objs := []*databasev1.ObjectSnapshot{
		{Group: "g", Kind: "stream", Name: "foo"},
		{Group: "g", Kind: "stream", Name: "bar"},
	}
	repo := &mockMetadataRepo{
		snapshotRules: rules, snapshotObjects: objs, snapshotFound: true, cachedGroups: []string{"g"},
	}
	s := &server{metadataRepo: repo, curNode: &databasev1.Node{Metadata: &commonv1.Metadata{Name: "node-1"}}}
	stream := &fakeSchemaStream{ctx: context.Background()}

	require.NoError(t, s.StreamGroupSchemaState(&databasev1.NodeSchemaStateRequest{}, stream))

	var tableEvents, objectEvents, trailers, lastTableIdx, firstObjectIdx int
	firstObjectIdx = -1
	var concatenated []*databasev1.IndexRule
	var trailer *databasev1.SchemaSnapshotTrailer
	for i, e := range stream.events {
		switch ev := e.GetEvent().(type) {
		case *databasev1.SchemaSnapshotEvent_RuleTable:
			tableEvents++
			lastTableIdx = i
			assert.LessOrEqual(t, len(ev.RuleTable.GetRules()), ruleTableChunkSize, "each chunk is bounded by ruleTableChunkSize")
			concatenated = append(concatenated, ev.RuleTable.GetRules()...)
		case *databasev1.SchemaSnapshotEvent_Object:
			objectEvents++
			if firstObjectIdx < 0 {
				firstObjectIdx = i
			}
		case *databasev1.SchemaSnapshotEvent_Trailer:
			trailers++
			trailer = ev.Trailer
		}
	}

	assert.Equal(t, 3, tableEvents, "120 rules at 50/chunk -> 3 rule-table events")
	assert.Equal(t, len(objs), objectEvents, "objects are still one event each")
	require.Equal(t, 1, trailers)
	assert.Equal(t, uint32(len(objs)), trailer.GetObjectCount(), "object_count counts objects, not rule-table chunks")
	assert.Less(t, lastTableIdx, firstObjectIdx, "all rule-table chunks precede the object events")

	require.Len(t, concatenated, ruleCount, "every rule survives the split")
	for i, r := range concatenated {
		assert.Equal(t, fmt.Sprintf("r%d", i), r.GetMetadata().GetName(), "concatenation preserves order across chunks")
	}
}
