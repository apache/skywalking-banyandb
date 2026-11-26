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

package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

const (
	heldGroup     = "default"
	knownStream   = "sw"
	unknownStream = "not_registered"
	unheldGroup   = "any-name-a-writer-sends"
)

// dropRepository holds heldGroup and knownStream in it. A dropped row never reaches the
// group's storage, so the group supplies none.
type dropRepository struct{ resourceSchema.Repository }

func (dropRepository) LoadGroup(name string) (resourceSchema.Group, bool) {
	return nil, name == heldGroup
}

func (dropRepository) LoadResource(metadata *commonv1.Metadata) (resourceSchema.Resource, bool) {
	if metadata.GetGroup() != heldGroup || metadata.GetName() != knownStream {
		return nil, false
	}
	return streamResource{s: &stream{schema: &databasev1.Stream{
		Metadata:    metadata,
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "searchable", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
	}}}, true
}

type streamResource struct {
	resourceSchema.Resource
	s *stream
}

func (r streamResource) Delegated() resourceSchema.IndexListener { return r.s }

func streamWrite(group, name string, ts time.Time) *streamv1.InternalWriteRequest {
	return &streamv1.InternalWriteRequest{Request: &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: group, Name: name},
		Element:  &streamv1.ElementValue{ElementId: "e1", Timestamp: timestamppb.New(ts)},
	}}
}

// dropBatch has a row for each reason a stream write listener drops a row for before the row
// reaches the group's storage. A timestamp finer than a millisecond is out of range.
func dropBatch() []any {
	valid, invalid := time.Unix(1_800_000_000, 0), time.Unix(1_800_000_000, 1)
	return []any{
		streamWrite(heldGroup, knownStream, invalid),
		streamWrite(unheldGroup, knownStream, invalid),
		[]byte{0xff},
		42,
		streamWrite("deleted", knownStream, valid),
		streamWrite("deleted", knownStream, valid),
	}
}

// expectDrops expects the drops of dropBatch, with a group label only for the held group.
func expectDrops(t *testing.T) *meter.MockCounter {
	counter := meter.NewMockCounter(gomock.NewController(t))
	for _, d := range []struct {
		group  string
		reason common.DropReason
		times  int
	}{
		{heldGroup, common.DropReasonInvalidTimestamp, 1},
		{"", common.DropReasonInvalidTimestamp, 1},
		{"", common.DropReasonDecode, 1},
		{"", common.DropReasonInvalidType, 1},
		{"", common.DropReasonGroupNotFound, 2},
	} {
		counter.EXPECT().Inc(float64(1), d.group, string(d.reason)).Times(d.times)
	}
	return counter
}

func TestWriteCallbackCountsTheRowsItDrops(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{}}
	w := &writeCallback{
		l:          logger.GetLogger("test-stream-drop"),
		schemaRepo: repo,
		drops:      common.NewWriteDrops[resourceSchema.Group](expectDrops(t), repo),
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")

	recorder := common.NewWriteRecorder(time.Time{})
	w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(2, dropBatch()))
	require.Len(t, recorder.RowFailures(), 6, "a write with a recorder reports every dropped row to its caller instead of counting it")
}

func TestWriteQueueCallbackCountsTheRowsItDrops(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{}}
	counter := expectDrops(t)
	counter.EXPECT().Inc(float64(1), "", string(common.DropReasonInvalidType)) // the message that is not a batch
	w := &writeQueueCallback{
		l:          logger.GetLogger("test-stream-drop-liaison"),
		schemaRepo: repo,
		drops:      common.NewWriteDrops[resourceSchema.Group](counter, repo),
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")
	w.Rev(context.Background(), bus.NewMessage(2, "not a batch"))
	w.Rev(context.Background(), bus.NewMessage(3, []any{}))
}

// A stream checks the schema of a row after it has a table for it, so both listeners reach
// these reasons through processElements, which they share.
func TestProcessElementsTagsTheRowsItCannotStore(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{}}
	ts := time.Unix(1_800_000_000, 0)
	for _, c := range []struct {
		name string
		want common.DropReason
	}{
		{unknownStream, common.DropReasonSchemaNotFound},
		{knownStream, common.DropReasonInvalidElement}, // the element has no tag family
	} {
		writeEvent := streamWrite(heldGroup, c.name, ts)
		var docs index.Documents
		series := seriesDoc{docIDsAdded: map[uint64]struct{}{}}
		err := processElements(repo, &elements{}, writeEvent, ts.UnixNano(), &docs, &series, writeEvent.Request.Metadata, nil)
		require.Error(t, err, c.name)
		require.Equal(t, c.want, common.ReasonOf(err), c.name)
	}
}
