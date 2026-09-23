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
	"fmt"
	"slices"

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

// errSchemaIncompatible reports that a stream of this name already exists but
// does not have the shape the write path fills. It is distinguished from an
// ordinary failure because retrying cannot fix it: the stream has to be
// dropped and recreated by hand.
var errSchemaIncompatible = errors.New("the existing native log stream is not compatible with this version")

// schemaState is what one successful schema pass established. The shard count
// is the group's, not the flag's: the flag only proposes a value when the group
// is created, and every node after the first finds it already there.
type schemaState struct {
	// shardMismatch and ttlMismatch record that the running configuration
	// disagrees with what is persisted, so the caller can say so once rather
	// than leaving an inert flag looking effective.
	shardMismatch string
	ttlMismatch   string
	shardNum      uint32
}

// createSchema creates the group and the stream and reports what is actually in
// force. Both creations are idempotent: a process that finds them already there
// carries on, which is the normal case for every node after the first.
//
// "Already there" is not taken as "already correct", for either object.
//
// A write request carries its tags positionally -- modelv1.TagFamilyForWrite
// has no name, and neither do the values inside it -- so a stream whose
// families or tags are in a different order would accept every event and file
// each value under the wrong tag.
//
// The group matters for the same reason one step further out: the shard a write
// is routed to is computed modulo a shard count, and the storage layer does not
// range-check it. Routing on the flag while the group holds a different count
// produces shard directories above the group's own count, which segment
// loadShards skips on the next open -- so the events are written, acked,
// counted and queryable, and then silently invisible after an idle close or a
// restart. Both failures are counted as successes, which is worse than not
// writing at all.
func createSchema(ctx context.Context, repo metadata.Repo, shardNum, ttlDays uint32) (schemaState, error) {
	state := schemaState{shardNum: shardNum}
	group := &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: GroupName},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        shardNum,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: ttlDays},
		},
	}
	// Read before writing. Every node runs this on start and again on each
	// retry, and a create is broadcast to every schema server, so creating
	// first turns a settled cluster into a permanent write fan-out: 20 data
	// nodes against 3 servers issue 120 doomed inserts every retry interval.
	existing, getErr := repo.GroupRegistry().GetGroup(ctx, GroupName)
	switch {
	case getErr == nil:
		state = groupState(existing.GetResourceOpts(), shardNum, ttlDays)
	case errors.Is(getErr, schema.ErrGRPCResourceNotFound):
		if _, err := repo.GroupRegistry().CreateGroup(ctx, group); err != nil &&
			!errors.Is(err, schema.ErrGRPCAlreadyExists) {
			return state, err
		}
		// The create says nothing reliable about what is now in force. It
		// reports AlreadyExists only when every schema server rejects it, so a
		// create that one server accepted returns nil although the group
		// already existed elsewhere with another shard count. Routing divides
		// by that count, and a count taken from this node's flag would send
		// events to shards the group does not have: written, acked, counted,
		// and invisible after the next open.
		stored, storedErr := repo.GroupRegistry().GetGroup(ctx, GroupName)
		if storedErr != nil {
			return state, storedErr
		}
		state = groupState(stored.GetResourceOpts(), shardNum, ttlDays)
	default:
		return state, getErr
	}

	want := streamSpec()
	got, getErr := repo.StreamRegistry().GetStream(ctx, want.Metadata)
	switch {
	case getErr == nil:
		return state, compatible(want, got)
	case !errors.Is(getErr, schema.ErrGRPCResourceNotFound):
		return state, getErr
	}
	if _, err := repo.StreamRegistry().CreateStream(ctx, want); err != nil &&
		!errors.Is(err, schema.ErrGRPCAlreadyExists) {
		return state, err
	}
	stored, storedErr := repo.StreamRegistry().GetStream(ctx, want.Metadata)
	if storedErr != nil {
		return state, storedErr
	}
	return state, compatible(want, stored)
}

// groupState reads what the persisted group actually says and notes where the
// running configuration disagrees with it. Neither value is updated in place:
// raising a group's shard count is a data operation, not something a node
// should do to a shared group on its way up.
func groupState(opts *commonv1.ResourceOpts, shardNum, ttlDays uint32) schemaState {
	state := schemaState{shardNum: opts.GetShardNum()}
	if state.shardNum == 0 {
		// Nothing persisted, so the flag is the only value left to divide by.
		state.shardNum = shardNum
	}
	if state.shardNum != shardNum {
		state.shardMismatch = fmt.Sprintf(
			"--logging-native-shard-num=%d is ignored; %q already exists with %d shards, and routing follows the group",
			shardNum, GroupName, state.shardNum)
	}
	if ttl := opts.GetTtl(); ttl != nil &&
		(ttl.GetUnit() != commonv1.IntervalRule_UNIT_DAY || ttl.GetNum() != ttlDays) {
		state.ttlMismatch = fmt.Sprintf(
			"--logging-native-ttl-days=%d is ignored; %q already exists with a retention of %d %s",
			ttlDays, GroupName, ttl.GetNum(), ttl.GetUnit())
	}
	return state
}

// compatible reports whether an existing stream can be written to by the
// request Sink.build produces. Names are compared in order rather than as
// sets, because order is the whole of the mapping: the nth value in the nth
// family is the nth declared tag, and nothing in the request says otherwise.
func compatible(want, got *databasev1.Stream) error {
	wantFamilies, gotFamilies := want.GetTagFamilies(), got.GetTagFamilies()
	if len(wantFamilies) != len(gotFamilies) {
		return fmt.Errorf("%w: it has %d tag families, want %d",
			errSchemaIncompatible, len(gotFamilies), len(wantFamilies))
	}
	for i, wf := range wantFamilies {
		gf := gotFamilies[i]
		if wf.GetName() != gf.GetName() {
			return fmt.Errorf("%w: tag family %d is %q, want %q",
				errSchemaIncompatible, i, gf.GetName(), wf.GetName())
		}
		wantTags, gotTags := wf.GetTags(), gf.GetTags()
		if len(wantTags) != len(gotTags) {
			return fmt.Errorf("%w: tag family %q has %d tags, want %d",
				errSchemaIncompatible, wf.GetName(), len(gotTags), len(wantTags))
		}
		for j, wt := range wantTags {
			gt := gotTags[j]
			if wt.GetName() != gt.GetName() {
				return fmt.Errorf("%w: tag %d of family %q is %q, want %q",
					errSchemaIncompatible, j, wf.GetName(), gt.GetName(), wt.GetName())
			}
			if wt.GetType() != gt.GetType() {
				return fmt.Errorf("%w: tag %q is a %s, want %s",
					errSchemaIncompatible, wt.GetName(), gt.GetType(), wt.GetType())
			}
		}
	}
	// The entity decides the shard, so a difference here would send events to a
	// shard no reader looks in for them.
	wantEntity, gotEntity := want.GetEntity().GetTagNames(), got.GetEntity().GetTagNames()
	if !slices.Equal(wantEntity, gotEntity) {
		return fmt.Errorf("%w: its entity is %v, want %v",
			errSchemaIncompatible, gotEntity, wantEntity)
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
