// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	// oapBodyBytes is the size of one stored file, as SkyWalking OAP's ai-agent keeps session files.
	oapBodyBytes = 1500 * 1000
	// oapLimit is the LIMIT OAP reads a session with.
	oapLimit = 10000
	// oapQueryAllowance is the per-query allowance of a 64 MiB pool.
	oapQueryAllowance = 8 << 20
)

// buildOAPStream stores copies rows that share one timestamp, each with a 1.5 MB body and its own series and part, in
// the shape of the ai-agent stream: a storage-only family with a binary body, a digest and an entity, and a searchable
// family with an integer.
func buildOAPStream(t *testing.T, copies int) (*stream, timestamp.TimeRange, [][]*modelv1.TagValue) {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	ts := time.Now().Add(-time.Hour).Truncate(time.Hour).UnixNano()
	db := openDatabaseWithTTLDays(t, t.TempDir(), 2)
	t.Cleanup(func() { _ = db.Close() })
	seg, err := db.CreateSegmentIfNotExist(time.Unix(0, ts))
	require.NoError(t, err)
	defer seg.DecRef()
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)

	body := make([]byte, oapBodyBytes)
	for i := range body {
		body[i] = byte(i*131 + i>>8)
	}
	var entities [][]*modelv1.TagValue
	var entityDocs index.Documents
	for i := 0; i < copies; i++ {
		entityName := "session-" + string(rune('a'+i))
		series := &pbv1.Series{
			Subject:      "oap-files",
			EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityName}}}},
		}
		require.NoError(t, series.Marshal())
		entities = append(entities, series.EntityValues)
		entityDocs = append(entityDocs, index.Document{DocID: uint64(series.ID), EntityValues: series.Buffer})
		// One call per row: every row is flushed into a part of its own.
		tst.mustAddElements(&elements{
			seriesIDs:  []common.SeriesID{series.ID},
			timestamps: []int64{ts},
			elementIDs: []uint64{convert.HashStr(entityName)},
			tagFamilies: [][]tagValues{{
				{tag: "storage-only", values: []*tagValue{
					{tag: "body", value: body, valueType: pbv1.ValueTypeBinaryData},
					{tag: "digest", value: []byte("sha256:" + entityName), valueType: pbv1.ValueTypeStr},
					{tag: entityTagName, value: []byte(entityName), valueType: pbv1.ValueTypeStr},
				}},
				{tag: "searchable", values: []*tagValue{
					{tag: "seq", value: convert.Int64ToBytes(int64(i)), valueType: pbv1.ValueTypeInt64},
				}},
			}},
		})
	}
	seg.IndexDB().Insert(entityDocs)

	schema := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "oap-files", Group: "test"},
		Entity:   &databasev1.Entity{TagNames: []string{entityTagName}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "storage-only", Tags: []*databasev1.TagSpec{
				{Name: "body", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
				{Name: "digest", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: entityTagName, Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
			{Name: "searchable", Tags: []*databasev1.TagSpec{{Name: "seq", Type: databasev1.TagType_TAG_TYPE_INT}}},
		},
	}
	s := &stream{schema: schema, l: logger.GetLogger("test-oap-budget"), pm: protector.Nop{}, vectorized: vstream.DefaultConfig()}
	s.name, s.group = "oap-files", "test"
	var is indexSchema
	is.parse(schema)
	s.indexSchema.Store(is)
	s.tsdb.Store(db)
	return s, timestamp.NewInclusiveTimeRange(time.Unix(0, ts), time.Unix(0, ts)), entities
}

// runOAPQuery reads every row with the OAP query shape (time order, LIMIT 10000, body projected) under a lease of
// the given budget, admitted as a scan whose results are charged as they are retained: a LIMIT of 10000 is far
// beyond what the admission window of one query allows. It returns the rows read, the peak the lease reached above its admission overhead and the error.
func runOAPQuery(t *testing.T, s *stream, tr timestamp.TimeRange, entities [][]*modelv1.TagValue, budget *protector.QueryBudget) (int, uint64, uint64, error) {
	t.Helper()
	admitted, release, err := budget.AdmitScanContext(context.Background())
	require.NoError(t, err)
	defer release()
	lease, ok := query.BudgetLeaseFromContext(admitted)
	require.True(t, ok)
	peak := &peakLease{BudgetLease: lease}
	ctx := query.WithBudgetLease(admitted, peak)
	base := lease.Used()

	sqo := model.StreamQueryOptions{
		Name:      "oap-files",
		TimeRange: &tr,
		Entities:  entities,
		TagProjection: []model.TagProjection{
			{Family: "storage-only", Names: []string{"body", "digest", entityTagName}},
			{Family: "searchable", Names: []string{"seq"}},
		},
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxElementSize: oapLimit,
	}
	rows, queryErr := drainOAPPipeline(ctx, t, s, sqo)
	return rows, peak.peak.Load() - base, lease.Limit() - base, queryErr
}

func drainOAPPipeline(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions) (int, error) {
	t.Helper()
	src, err := s.queryVectorized(ctx, sqo)
	if err != nil {
		return 0, err
	}
	schema := src.Schema()
	pipeline, err := vstream.BuildStreamMergePipeline(&testVecSource{src: src, schema: schema}, schema, true,
		0, oapLimit, vstream.DefaultConfig().BatchSize, sqo.MaxElementSize)
	require.NoError(t, err)
	if err = pipeline.Init(ctx); err != nil {
		_ = pipeline.Close()
		return 0, err
	}
	rows := 0
	for {
		batch, nextErr := pipeline.Next(ctx)
		if nextErr != nil {
			_ = pipeline.Close()
			return rows, nextErr
		}
		if batch == nil {
			break
		}
		rows += batch.ActiveLen()
	}
	require.NoError(t, pipeline.Close())
	return rows, nil
}

// TestQueryVectorized_LargeBodiesAreChargedBySize is the regression for SkyWalking OAP's ai-agent E2E, which stores
// whole files as a binary tag and reads one back. A 1.5 MB value used to be charged about eight times its size, so
// a single row exhausted the 8 MiB per-query allowance.
func TestQueryVectorized_LargeBodiesAreChargedBySize(t *testing.T) {
	t.Run("one copy fits an 8 MiB allowance", func(t *testing.T) {
		defer protector.SetHostMemoryForTest(0)()
		s, tr, entities := buildOAPStream(t, 1)
		budget := protector.NewQueryBudget(nil)
		require.Equal(t, uint64(oapQueryAllowance), budget.MaxQuery())
		rows, peak, allowance, err := runOAPQuery(t, s, tr, entities, budget)
		require.NoError(t, err)
		require.Equal(t, 1, rows)
		t.Logf("one copy: peak charge %d of an allowance of %d", peak, allowance)
		require.Less(t, peak, uint64(4<<20), "a 1.5 MB body must not be charged several times over")
		require.Greater(t, peak, uint64(oapBodyBytes), "the body itself must be charged")
	})

	t.Run("five copies exceed an 8 MiB allowance", func(t *testing.T) {
		defer protector.SetHostMemoryForTest(0)()
		s, tr, entities := buildOAPStream(t, 5)
		_, _, _, err := runOAPQuery(t, s, tr, entities, protector.NewQueryBudget(nil))
		require.ErrorIs(t, err, protector.ErrQueryResourceExhausted,
			"7.5 MB of bodies plus the block bytes read to produce them cannot fit 8 MiB")
	})

	t.Run("five copies fit a pool sized from a 16 GiB host", func(t *testing.T) {
		defer protector.SetHostMemoryForTest(16 << 30)()
		s, tr, entities := buildOAPStream(t, 5)
		budget := protector.NewQueryBudget(protector.Nop{})
		require.Equal(t, uint64(3<<30)/8, budget.MaxQuery())
		rows, peak, _, err := runOAPQuery(t, s, tr, entities, budget)
		require.NoError(t, err)
		require.Equal(t, 5, rows)
		t.Logf("five copies on a 16 GiB host: peak charge %d", peak)
	})
}
