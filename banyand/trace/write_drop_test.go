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

package trace

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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

const (
	heldGroup    = "sw_trace"
	segment      = "segment" // registered, with a timestamp tag
	noTimestamp  = "no_ts"   // registered, without its timestamp tag: rows do not match it
	unknownTrace = "unknown" // not registered
	unheldGroup  = "any-name-a-writer-sends"
)

// dropRepository holds heldGroup and the traces segment and noTimestamp in it. A row is
// dropped before it reaches storage, so the group supplies none.
type dropRepository struct{ resourceSchema.Repository }

func (dropRepository) LoadGroup(name string) (resourceSchema.Group, bool) {
	return nil, name == heldGroup
}

func (dropRepository) LoadResource(metadata *commonv1.Metadata) (resourceSchema.Resource, bool) {
	if metadata.GetGroup() != heldGroup {
		return nil, false
	}
	schema := &databasev1.Trace{Metadata: metadata, TimestampTagName: "ts"}
	switch metadata.GetName() {
	case segment:
		schema.Tags = []*databasev1.TraceTagSpec{{Name: "ts", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP}}
	case noTimestamp:
	default:
		return nil, false
	}
	return traceResource{t: &trace{schema: schema, name: metadata.GetName()}}, true
}

type traceResource struct {
	resourceSchema.Resource
	t *trace
}

func (r traceResource) Delegated() resourceSchema.IndexListener { return r.t }

func traceWrite(group, name string, tags ...*modelv1.TagValue) *tracev1.InternalWriteRequest {
	return &tracev1.InternalWriteRequest{Request: &tracev1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: group, Name: name},
		Tags:     tags,
	}}
}

// dropBatch has a row for each reason a trace write listener drops a row for before it
// stores anything. A timestamp finer than a millisecond is out of range.
func dropBatch() []any {
	invalid := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(time.Unix(1_800_000_000, 1))}}
	return []any{
		[]byte{0xff},
		42,
		traceWrite("deleted", segment),
		traceWrite(unheldGroup, segment),
		traceWrite(heldGroup, unknownTrace),
		traceWrite(heldGroup, noTimestamp),
		traceWrite(heldGroup, segment, invalid),
	}
}

// expectDrops expects the drops of dropBatch, with a group label only for the held group. A
// row for a group the node does not hold is group_not_found, though the trace is looked up first.
func expectDrops(t *testing.T) *meter.MockCounter {
	counter := meter.NewMockCounter(gomock.NewController(t))
	for _, d := range []struct {
		group  string
		reason common.DropReason
		times  int
	}{
		{"", common.DropReasonDecode, 1},
		{"", common.DropReasonInvalidType, 1},
		{"", common.DropReasonGroupNotFound, 2},
		{heldGroup, common.DropReasonSchemaNotFound, 1},
		{heldGroup, common.DropReasonInvalidElement, 1},
		{heldGroup, common.DropReasonInvalidTimestamp, 1},
	} {
		counter.EXPECT().Inc(float64(1), d.group, string(d.reason)).Times(d.times)
	}
	return counter
}

func TestWriteCallbackCountsTheRowsItDrops(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{}}
	w := &writeCallback{
		l:          logger.GetLogger("test-trace-drop"),
		schemaRepo: repo,
		drops:      common.NewWriteDrops[resourceSchema.Group](expectDrops(t), repo),
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")

	recorder := common.NewWriteRecorder(time.Time{})
	w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(2, dropBatch()))
	require.Len(t, recorder.RowFailures(), 7, "a write with a recorder reports every dropped row to its caller instead of counting it")
}

func TestWriteQueueCallbackCountsTheRowsItDrops(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{}}
	counter := expectDrops(t)
	counter.EXPECT().Inc(float64(1), "", string(common.DropReasonInvalidType)) // the message that is not a batch
	w := &writeQueueCallback{
		l:          logger.GetLogger("test-trace-drop-liaison"),
		schemaRepo: repo,
		drops:      common.NewWriteDrops[resourceSchema.Group](counter, repo),
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")
	w.Rev(context.Background(), bus.NewMessage(2, "not a batch"))
	w.Rev(context.Background(), bus.NewMessage(3, []any{}))
}
