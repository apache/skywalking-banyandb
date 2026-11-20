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

package measure

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

const (
	heldGroup      = "sw_metric"
	knownMeasure   = "service_cpm_minute"
	unheldGroup    = "any-name-a-writer-sends"
	deletedGroup   = "deleted"
	unknownMeasure = "not_registered"
)

// dropRepository holds heldGroup, whose storage is db, and knownMeasure in it. A row reaches
// the schema checks before db is used, so db is never called.
type dropRepository struct {
	resourceSchema.Repository
	db io.Closer
}

func (r dropRepository) LoadGroup(name string) (resourceSchema.Group, bool) {
	if name != heldGroup {
		return nil, false
	}
	return heldGroupStub{db: r.db}, true
}

func (dropRepository) LoadResource(metadata *commonv1.Metadata) (resourceSchema.Resource, bool) {
	if metadata.GetGroup() != heldGroup || metadata.GetName() != knownMeasure {
		return nil, false
	}
	return measureResource{m: &measure{schema: &databasev1.Measure{
		Metadata:    metadata,
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}}}},
	}}}, true
}

type heldGroupStub struct {
	resourceSchema.Group
	db io.Closer
}

func (g heldGroupStub) SupplyTSDB() io.Closer { return g.db }

type measureResource struct {
	resourceSchema.Resource
	m *measure
}

func (r measureResource) Delegated() resourceSchema.IndexListener { return r.m }

// unusedTSDB stands for a group's storage that a dropped row never reaches. The listener still
// ticks every group a row named, so Tick is all it answers.
type unusedTSDB struct{ storage.TSDB[*tsTable, option] }

func (unusedTSDB) Tick(int64) {}

func measureWrite(group, name string, ts time.Time) *measurev1.InternalWriteRequest {
	return &measurev1.InternalWriteRequest{Request: &measurev1.WriteRequest{
		Metadata:  &commonv1.Metadata{Group: group, Name: name},
		DataPoint: &measurev1.DataPointValue{Timestamp: timestamppb.New(ts)},
	}}
}

// dropBatch has a row for each reason a measure write listener drops a row for before it
// stores anything. A timestamp finer than a millisecond is out of range, and a data point
// without tag families does not match its schema.
func dropBatch() []any {
	valid, invalid := time.Unix(1_800_000_000, 0), time.Unix(1_800_000_000, 1)
	return []any{
		measureWrite(heldGroup, knownMeasure, invalid),
		measureWrite(unheldGroup, knownMeasure, invalid),
		[]byte{0xff},
		42,
		measureWrite(deletedGroup, knownMeasure, valid),
		measureWrite(deletedGroup, knownMeasure, valid),
		measureWrite(heldGroup, unknownMeasure, valid),
		measureWrite(heldGroup, knownMeasure, valid),
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
		{heldGroup, common.DropReasonSchemaNotFound, 1},
		{heldGroup, common.DropReasonInvalidElement, 1},
	} {
		counter.EXPECT().Inc(float64(1), d.group, string(d.reason)).Times(d.times)
	}
	return counter
}

func TestWriteCallbackCountsTheRowsItDrops(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{db: unusedTSDB{}}}
	w := &writeCallback{
		l:     logger.GetLogger("test-measure-drop"),
		drops: common.NewWriteDrops[resourceSchema.Group](expectDrops(t), repo),
	}
	w.schemaRepo = repo

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")

	recorder := common.NewWriteRecorder(time.Time{})
	w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(2, dropBatch()))
	require.Len(t, recorder.RowFailures(), 8, "a write with a recorder reports every dropped row to its caller instead of counting it")
}

func TestWriteQueueCallbackCountsTheRowsItDrops(t *testing.T) {
	repo := &schemaRepo{Repository: dropRepository{db: &wqueue.Queue[*tsTable, option]{}}}
	counter := expectDrops(t)
	counter.EXPECT().Inc(float64(1), "", string(common.DropReasonInvalidType)) // the message that is not a batch
	w := &writeQueueCallback{
		l:          logger.GetLogger("test-measure-drop-liaison"),
		schemaRepo: repo,
		drops:      common.NewWriteDrops[resourceSchema.Group](counter, repo),
	}

	reply := w.Rev(context.Background(), bus.NewMessage(1, dropBatch()))
	require.Nil(t, reply.Data(), "counting a drop leaves the acknowledgement as it was")
	w.Rev(context.Background(), bus.NewMessage(2, "not a batch"))
	w.Rev(context.Background(), bus.NewMessage(3, []any{}))
}
