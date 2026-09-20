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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const (
	// GroupName holds BanyanDB's own log events. It cannot be _monitoring:
	// group names are unique across catalogs and that one is a measure group,
	// while log events are elements of a stream.
	GroupName = "_monitoring_log"
	// StreamName is the single stream every role writes to. The node_type tag
	// separates the roles, so one schema serves all of them.
	StreamName = "log"

	searchableFamily = "searchable"
	dataFamily       = "data"

	tagNodeID      = "node_id"
	tagNodeType    = "node_type"
	tagModule      = "module"
	tagLevel       = "level"
	tagGRPCAddress = "grpc_address"
	tagHTTPAddress = "http_address"
	tagMessage     = "message"
	tagLogID       = "log_id"
	tagFields      = "fields"
)

// searchableTags is the order the write path fills. Tag families are
// positional, so this list and the one built in Sink.build must agree.
var searchableTags = []string{
	tagNodeID, tagNodeType, tagModule, tagLevel,
	tagGRPCAddress, tagHTTPAddress, tagMessage, tagLogID,
}

// entityTags is the series key. level is in it so that a query can select one
// severity without a scan; module is not, because module strings splice in
// group, measure and task names and would make the series count unbounded.
//
// Entity tags accept only equality and set membership, so selecting several
// levels at once uses IN rather than a negation.
var entityTags = []string{tagNodeID, tagLevel}

// createSchema creates the group and the stream. Both are idempotent: a
// process that finds them already there carries on, which is the normal case
// for every node after the first.
func createSchema(ctx context.Context, repo metadata.Repo, shardNum, ttlDays uint32) error {
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: GroupName},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        shardNum,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: ttlDays},
		},
	}
	if _, err := repo.GroupRegistry().CreateGroup(ctx, group); err != nil &&
		!errors.Is(err, schema.ErrGRPCAlreadyExists) {
		return err
	}
	if _, err := repo.StreamRegistry().CreateStream(ctx, streamSpec()); err != nil &&
		!errors.Is(err, schema.ErrGRPCAlreadyExists) {
		return err
	}
	return nil
}

// streamSpec is the stream definition. There are no index rules in this
// version: an unindexed tag is still filterable, it is evaluated after the
// scan rather than through a posting list.
func streamSpec() *databasev1.Stream {
	searchable := make([]*databasev1.TagSpec, 0, len(searchableTags))
	for _, name := range searchableTags {
		searchable = append(searchable, &databasev1.TagSpec{
			Name: name,
			Type: databasev1.TagType_TAG_TYPE_STRING,
		})
	}
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: StreamName, Group: GroupName},
		Entity:   &databasev1.Entity{TagNames: entityTags},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: searchableFamily, Tags: searchable},
			{
				Name: dataFamily,
				Tags: []*databasev1.TagSpec{{
					// Only the keys with no tag of their own land here, so
					// nothing is stored twice and a call site that adds a field
					// does not need a schema change to keep it.
					Name: tagFields,
					Type: databasev1.TagType_TAG_TYPE_DATA_BINARY,
				}},
			},
		},
	}
}
