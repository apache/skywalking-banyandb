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

package logging

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// TestCompatibleRejectsAReorderedSchema is the falsifying assertion for the
// comparison. A set-wise check would accept every case below except the last
// two: the names are all still present, only their positions moved. Position is
// the entire mapping -- a write request carries no tag names at all -- so an
// accepted reorder means every event is filed under the wrong tag and counted
// as written.
func TestCompatibleRejectsAReorderedSchema(t *testing.T) {
	tests := []struct {
		mutate func(*databasev1.Stream)
		name   string
	}{
		{
			name: "two searchable tags swapped",
			mutate: func(s *databasev1.Stream) {
				tags := s.TagFamilies[0].Tags
				tags[2], tags[3] = tags[3], tags[2]
			},
		},
		{
			name: "the two families swapped",
			mutate: func(s *databasev1.Stream) {
				s.TagFamilies[0], s.TagFamilies[1] = s.TagFamilies[1], s.TagFamilies[0]
			},
		},
		{
			name: "entity tags reordered",
			mutate: func(s *databasev1.Stream) {
				e := s.Entity.TagNames
				e[0], e[1] = e[1], e[0]
			},
		},
		{
			name: "a tag inserted ahead of the rest",
			mutate: func(s *databasev1.Stream) {
				s.TagFamilies[0].Tags = append([]*databasev1.TagSpec{{
					Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING,
				}}, s.TagFamilies[0].Tags...)
			},
		},
		{
			name: "a tag appended after the rest",
			mutate: func(s *databasev1.Stream) {
				s.TagFamilies[0].Tags = append(s.TagFamilies[0].Tags, &databasev1.TagSpec{
					Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING,
				})
			},
		},
		{
			name: "the data tag stored as a string",
			mutate: func(s *databasev1.Stream) {
				s.TagFamilies[1].Tags[0].Type = databasev1.TagType_TAG_TYPE_STRING
			},
		},
		{
			name: "a family dropped",
			mutate: func(s *databasev1.Stream) {
				s.TagFamilies = s.TagFamilies[:1]
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing, ok := proto.Clone(streamSpec()).(*databasev1.Stream)
			if !ok {
				t.Fatal("clone did not return a stream")
			}
			tt.mutate(existing)
			err := compatible(streamSpec(), existing)
			if err == nil {
				t.Fatalf("a stream with %s was accepted; every event written to it "+
					"would be filed under the wrong tag and counted as stored", tt.name)
			}
			if !errors.Is(err, errSchemaIncompatible) {
				t.Fatalf("got %v, want it to wrap errSchemaIncompatible so the "+
					"caller can tell it apart from a retryable failure", err)
			}
		})
	}
}

// TestCompatibleAcceptsItsOwnSpec guards the other direction: the comparison
// must not be so strict that the normal case -- every node after the first
// finding the stream already there -- is reported as a mismatch.
func TestCompatibleAcceptsItsOwnSpec(t *testing.T) {
	if err := compatible(streamSpec(), streamSpec()); err != nil {
		t.Fatalf("the spec this version creates was rejected as incompatible with "+
			"itself: %v", err)
	}
}

// TestGroupStateReportsARetentionMismatch keeps an inert flag from looking
// effective: a node that finds the group already there does not change its
// retention, so it says so once.
func TestGroupStateReportsARetentionMismatch(t *testing.T) {
	dayTTL := func(n uint32) *commonv1.IntervalRule {
		return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
	}
	tests := []struct {
		opts       *commonv1.ResourceOpts
		name       string
		flagTTL    uint32
		wantTTLMsg bool
	}{
		{name: "flag agrees with the group", opts: &commonv1.ResourceOpts{ShardNum: 1, Ttl: dayTTL(7)}, flagTTL: 7},
		{name: "ttl differs", opts: &commonv1.ResourceOpts{ShardNum: 1, Ttl: dayTTL(30)}, flagTTL: 7, wantTTLMsg: true},
		// A group created by an earlier version with more shards is still
		// usable: every node writes to shard 0, which every group has.
		{name: "group has more shards", opts: &commonv1.ResourceOpts{ShardNum: 2, Ttl: dayTTL(7)}, flagTTL: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := groupState(tt.opts, tt.flagTTL)
			if (got.ttlMismatch != "") != tt.wantTTLMsg {
				t.Fatalf("ttlMismatch = %q, want a message: %v", got.ttlMismatch, tt.wantTTLMsg)
			}
		})
	}
}

// repoWith builds a metadata repository whose registries are the given mocks.
func repoWith(ctrl *gomock.Controller, groups *schema.MockGroup, streams *schema.MockStream) metadata.Repo {
	repo := metadata.NewMockRepo(ctrl)
	repo.EXPECT().GroupRegistry().Return(groups).AnyTimes()
	repo.EXPECT().StreamRegistry().Return(streams).AnyTimes()
	return repo
}

// TestCreateMakesASingleShardGroup holds the group to the shape native metrics
// use: one shard, which every node writes its own logs to. A create is
// broadcast to every schema server and says nothing reliable about what is in
// force, so the retention is read back from storage after it.
func TestCreateMakesASingleShardGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	groups := schema.NewMockGroup(ctrl)
	streams := schema.NewMockStream(ctrl)
	stored := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: GroupName},
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
			Ttl:      &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 30},
		},
	}
	var created *commonv1.Group
	gomock.InOrder(
		groups.EXPECT().GetGroup(gomock.Any(), GroupName).Return(nil, schema.ErrGRPCResourceNotFound),
		groups.EXPECT().CreateGroup(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, g *commonv1.Group) (int64, error) {
				created = g
				return 1, nil
			}),
		groups.EXPECT().GetGroup(gomock.Any(), GroupName).Return(stored, nil),
	)
	streams.EXPECT().GetStream(gomock.Any(), gomock.Any()).Return(streamSpec(), nil)

	state, err := createSchema(context.Background(), repoWith(ctrl, groups, streams), 7)
	if err != nil {
		t.Fatalf("createSchema: %v", err)
	}
	if got := created.GetResourceOpts().GetShardNum(); got != 1 {
		t.Errorf("the group was created with %d shards, want 1", got)
	}
	if state.ttlMismatch == "" {
		t.Error("the disagreement between the flag and the stored retention was not reported")
	}
}

// TestSettledSchemaIsNotRewritten keeps the retry from becoming a write
// fan-out. Every node runs this on start and on every retry, and a create
// reaches every schema server, so creating first makes a settled cluster issue
// doomed inserts forever.
func TestSettledSchemaIsNotRewritten(t *testing.T) {
	ctrl := gomock.NewController(t)
	groups := schema.NewMockGroup(ctrl)
	streams := schema.NewMockStream(ctrl)
	groups.EXPECT().GetGroup(gomock.Any(), GroupName).Return(&commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: GroupName},
		ResourceOpts: &commonv1.ResourceOpts{ShardNum: 2, Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7}},
	}, nil)
	streams.EXPECT().GetStream(gomock.Any(), gomock.Any()).Return(streamSpec(), nil)
	// No CreateGroup and no CreateStream are expected: the mock fails the test
	// if either is called.

	if _, err := createSchema(context.Background(), repoWith(ctrl, groups, streams), 7); err != nil {
		t.Fatalf("createSchema: %v", err)
	}
}
