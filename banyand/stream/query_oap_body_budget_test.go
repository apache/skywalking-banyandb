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

package stream

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	oapBodyBytes = 1_500_000
	oapLimit     = 10000
	oapStream    = "oap-session-file"
	oapEntity    = "entity"
)

// buildOAPBodyStream writes one row per copy at the same timestamp, each in its own
// part, using the OAP ai-agent shape: a storage-only family holding a multi-MB binary
// body and a searchable family holding an int tag.
func buildOAPBodyStream(t *testing.T, copies int) (*stream, timestamp.TimeRange, [][]*modelv1.TagValue) {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	db := openDatabaseWithTTLDays(t, t.TempDir(), 2)
	t.Cleanup(func() { _ = db.Close() })
	ts := time.Now().Add(-time.Hour).Truncate(time.Second).UnixNano()
	series := &pbv1.Series{
		Subject:      oapStream,
		EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "session-1"}}}},
	}
	require.NoError(t, series.Marshal())
	seg, err := db.CreateSegmentIfNotExist(time.Unix(0, ts))
	require.NoError(t, err)
	defer seg.DecRef()
	seg.IndexDB().Insert(index.Documents{{DocID: uint64(series.ID), EntityValues: series.Buffer}})
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	body := []byte(strings.Repeat("b", oapBodyBytes))
	for i := 0; i < copies; i++ {
		elems := &elements{
			seriesIDs:  []common.SeriesID{series.ID},
			timestamps: []int64{ts},
			elementIDs: []uint64{convert.HashStr(fmt.Sprintf("oap-%d", i))},
			tagFamilies: [][]tagValues{{
				{tag: "storage-only", values: []*tagValue{
					{tag: "body", value: body, valueType: pbv1.ValueTypeBinaryData},
					{tag: "digest", value: []byte(fmt.Sprintf("digest-%d", i)), valueType: pbv1.ValueTypeStr},
					{tag: oapEntity, value: []byte("session-1"), valueType: pbv1.ValueTypeStr},
				}},
				{tag: "searchable", values: []*tagValue{
					{tag: "seq", value: convert.Int64ToBytes(int64(i)), valueType: pbv1.ValueTypeInt64},
				}},
			}},
		}
		tst.mustAddElements(elems)
	}
	schema := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: oapStream, Group: testGroupName},
		Entity:   &databasev1.Entity{TagNames: []string{oapEntity}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "storage-only", Tags: []*databasev1.TagSpec{
				{Name: "body", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
				{Name: "digest", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: oapEntity, Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
			{Name: "searchable", Tags: []*databasev1.TagSpec{{Name: "seq", Type: databasev1.TagType_TAG_TYPE_INT}}},
		},
	}
	s := &stream{schema: schema, l: logger.GetLogger("test-oap-body"), pm: protector.Nop{}, vectorized: vstream.DefaultConfig()}
	s.name, s.group = oapStream, testGroupName
	var is indexSchema
	is.parse(schema)
	s.indexSchema.Store(is)
	s.tsdb.Store(db)
	return s, timestamp.NewInclusiveTimeRange(time.Unix(0, ts-int64(time.Second)), time.Unix(0, ts+int64(time.Second))), [][]*modelv1.TagValue{series.EntityValues}
}

// runOAPQuery reads every row back in time order with the body projected, under a
// lease admitted from budget exactly like the data-node processor, and returns the
// peak charge, the number of rows and the query error.
func runOAPQuery(t *testing.T, s *stream, tr timestamp.TimeRange, entities [][]*modelv1.TagValue, budget *protector.QueryBudget) (uint64, int, error) {
	t.Helper()
	admitted, release, err := budget.AdmitRequestContext(context.Background(), oapLimit, 0, 20)
	require.NoError(t, err)
	defer release()
	lease, ok := query.BudgetLeaseFromContext(admitted)
	require.True(t, ok)
	peak := &peakLease{BudgetLease: lease}
	ctx := query.WithBudgetLease(admitted, peak)
	sqo := model.StreamQueryOptions{
		Name:      oapStream,
		TimeRange: &tr,
		Entities:  entities,
		TagProjection: []model.TagProjection{
			{Family: "storage-only", Names: []string{"body", "digest", oapEntity}},
			{Family: "searchable", Names: []string{"seq"}},
		},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: oapLimit,
	}
	src, err := s.queryVectorized(ctx, sqo)
	if err != nil {
		return peak.peak.Load(), 0, err
	}
	schema := src.Schema()
	pipeline, err := vstream.BuildStreamMergePipeline(&testVecSource{src: src, schema: schema}, schema, false,
		0, oapLimit, vstream.DefaultConfig().BatchSize, sqo.MaxElementSize)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	rows := 0
	var queryErr error
	for queryErr == nil {
		batch, nextErr := pipeline.Next(ctx)
		if nextErr != nil {
			queryErr = nextErr
			break
		}
		if batch == nil {
			break
		}
		rows += batch.ActiveLen()
	}
	require.NoError(t, pipeline.Close())
	return peak.peak.Load(), rows, queryErr
}

// TestQueryVectorized_OAPBodyRowsFitQueryBudget reproduces OAP's ai-agent session
// file read: a multi-MB binary body projected with LIMIT 10000 in time order.
func TestQueryVectorized_OAPBodyRowsFitQueryBudget(t *testing.T) {
	// The default budget of a node without a memory limit and an unreadable host is
	// 64 MiB, so a query is allowed 8 MiB.
	restore := protector.SetHostMemoryForTest(func() uint64 { return 0 })
	defer restore()
	smallBudget := protector.NewQueryBudget(protector.Nop{})
	require.Equal(t, uint64(8<<20), smallBudget.MaxQuery())

	t.Run("one body is charged by its size", func(t *testing.T) {
		s, tr, entities := buildOAPBodyStream(t, 1)
		peak, rows, err := runOAPQuery(t, s, tr, entities, smallBudget)
		require.NoError(t, err)
		require.Equal(t, 1, rows)
		t.Logf("one copy: peak charge=%d", peak)
		require.Greater(t, peak, uint64(oapBodyBytes))
		require.Less(t, peak, uint64(4<<20), "a 1.5 MB body must not be charged at several times its size")
	})

	t.Run("an honest working set above the allowance is still rejected", func(t *testing.T) {
		s, tr, entities := buildOAPBodyStream(t, 5)
		_, _, err := runOAPQuery(t, s, tr, entities, smallBudget)
		require.ErrorIs(t, err, protector.ErrQueryResourceExhausted)
	})

	t.Run("a pool sized from a 16 GiB host admits the same query", func(t *testing.T) {
		restoreHost := protector.SetHostMemoryForTest(func() uint64 { return 16 << 30 })
		defer restoreHost()
		budget := protector.NewQueryBudget(protector.Nop{})
		require.Equal(t, uint64(3<<30), budget.Limit())
		s, tr, entities := buildOAPBodyStream(t, 5)
		peak, rows, err := runOAPQuery(t, s, tr, entities, budget)
		require.NoError(t, err)
		require.Equal(t, 5, rows)
		t.Logf("five copies: peak charge=%d", peak)
		require.Greater(t, peak, uint64(5*oapBodyBytes)-1)
	})
}
