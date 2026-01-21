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
	"errors"
	"io"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedserver"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	version              = "1.0.0"
	entityTagValuePrefix = "entity"
)

type parameter struct {
	batchCount     int
	timestampCount int
	seriesCount    int
	seriesToQuery  int
	startTimestamp int
	endTimestamp   int
}

var pList = [1]parameter{
	{
		batchCount:     10,
		timestampCount: 144,
		seriesCount:    100,
		seriesToQuery:  10,
		startTimestamp: 1410,
		endTimestamp:   1440,
	},
}

type databaseSupplier struct {
	database atomic.Value
}

func (dbs *databaseSupplier) SupplyTSDB() io.Closer {
	if v := dbs.database.Load(); v != nil {
		return v.(io.Closer)
	}
	return nil
}

func generateData(p parameter) []*dataPoints {
	dpsList := make([]*dataPoints, 0)
	for i := 0; i < p.batchCount; i++ {
		dps := &dataPoints{
			seriesIDs:   []common.SeriesID{},
			timestamps:  []int64{},
			versions:    []int64{},
			tagFamilies: [][]nameValues{},
			fields:      []nameValues{},
		}
		for j := 1; j <= p.timestampCount; j++ {
			timestamp := i*p.timestampCount + j
			unixTimestamp := time.Unix(int64(timestamp), 0).UnixNano()
			for k := 1; k <= p.seriesCount; k++ {
				dps.seriesIDs = append(dps.seriesIDs, common.SeriesID(k))
				dps.timestamps = append(dps.timestamps, unixTimestamp)
				dps.versions = append(dps.versions, int64(timestamp))

				tagFamilies := []nameValues{
					{
						name: "benchmark-family",
						values: []*nameValue{
							{
								name:      "entity-tag",
								valueType: pbv1.ValueTypeStr,
								value:     []byte(entityTagValuePrefix + strconv.Itoa(k)),
							},
						},
					},
				}
				dps.tagFamilies = append(dps.tagFamilies, tagFamilies)

				fields := nameValues{
					name: "benchmark-fields",
					values: []*nameValue{
						{
							name:      "value",
							valueType: pbv1.ValueTypeFloat64,
							value:     convert.Float64ToBytes(float64(k)*10.5 + float64(j)*0.1),
						},
					},
				}
				dps.fields = append(dps.fields, fields)
			}
		}
		dpsList = append(dpsList, dps)
	}
	return dpsList
}

func openDatabase(b *testing.B, path string, cache storage.Cache) storage.TSDB[*tsTable, option] {
	ir := storage.IntervalRule{
		Unit: storage.DAY,
		Num:  1,
	}
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:        1,
		Location:        path,
		TSTableCreator:  newTSTable,
		SegmentInterval: ir,
		TTL:             ir,
		Option: option{
			mergePolicy:        newMergePolicy(math.MaxInt32, math.MaxFloat64, run.Bytes(math.MaxInt64)),
			flushTimeout:       time.Hour,
			seriesCacheMaxSize: 0,
		},
	}

	group := "benchmark"
	db, err := storage.OpenTSDB(
		common.SetPosition(context.Background(), func(p common.Position) common.Position {
			p.Module = "measure"
			p.Database = "benchmark"
			return p
		}),
		opts, cache, group,
	)
	require.NoError(b, err)
	return db
}

func write(b *testing.B, p parameter, dpsList []*dataPoints) (storage.TSDB[*tsTable, option], storage.Cache, func()) {
	tmpPath, defFn := test.Space(require.New(b))
	cache := storage.NewServiceCache()
	db := openDatabase(b, tmpPath, cache)

	var docs index.Documents
	for i := 1; i <= p.seriesCount; i++ {
		entity := []*modelv1.TagValue{
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: entityTagValuePrefix + strconv.Itoa(i),
					},
				},
			},
		}
		series := &pbv1.Series{
			Subject:      "benchmark",
			EntityValues: entity,
		}
		err := series.Marshal()
		require.NoError(b, err)
		docs = append(docs, index.Document{
			DocID:        uint64(i),
			EntityValues: series.Buffer,
		})
	}

	seg, err := db.CreateSegmentIfNotExist(time.Unix(0, dpsList[0].timestamps[0]))
	require.NoError(b, err)
	seg.IndexDB().Insert(docs)
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(b, err)
	for i := range dpsList {
		tst.mustAddDataPoints(dpsList[i])
	}
	return db, cache, func() {
		db.Close()
		defFn()
	}
}

func generateMeasure(db storage.TSDB[*tsTable, option], cache storage.Cache) *measure {
	dbSupplier := &databaseSupplier{
		database: atomic.Value{},
	}
	dbSupplier.database.Store(db)

	entity := &databasev1.Entity{
		TagNames: []string{"entity-tag"},
	}
	tagFamily := &databasev1.TagFamilySpec{
		Name: "benchmark-family",
		Tags: []*databasev1.TagSpec{
			{
				Name: "entity-tag",
				Type: databasev1.TagType_TAG_TYPE_STRING,
			},
		},
	}
	fields := []*databasev1.FieldSpec{
		{
			Name:              "value",
			FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		},
	}
	schema := &databasev1.Measure{
		Entity:      entity,
		TagFamilies: []*databasev1.TagFamilySpec{tagFamily},
		Fields:      fields,
	}
	l := logger.GetLogger("bootstrap")
	ctx := context.Background()
	pipeline := queue.Local()
	metaSvc, err := embeddedserver.NewService(ctx)
	if err != nil {
		l.Fatal().Err(err).Msg("failed to initiate metadata service")
	}
	metricSvc := obsservice.NewMetricService(metaSvc, pipeline, "standalone", nil)
	pm := protector.NewMemory(metricSvc)

	m := &measure{
		schema: schema,
		c:      cache,
		pm:     pm,
	}
	if err := m.parseSpec(); err != nil {
		panic(err)
	}
	m.tsdb.Store(db)
	return m
}

func generateMeasureQueryOptions(p parameter) model.MeasureQueryOptions {
	timeRange := timestamp.TimeRange{
		Start:        time.Unix(int64(p.startTimestamp), 0),
		End:          time.Unix(int64(p.endTimestamp), 0),
		IncludeStart: true,
		IncludeEnd:   true,
	}
	entities := make([][]*modelv1.TagValue, 0)
	for i := 1; i <= p.seriesToQuery; i++ {
		entity := []*modelv1.TagValue{
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: entityTagValuePrefix + strconv.Itoa(i),
					},
				},
			},
		}
		entities = append(entities, entity)
	}
	return model.MeasureQueryOptions{
		Name:            "benchmark",
		TimeRange:       &timeRange,
		Entities:        entities,
		FieldProjection: []string{"value"},
	}
}

func (m *measure) QueryWithCache(ctx context.Context, mqo model.MeasureQueryOptions, cache storage.Cache) (mqr model.MeasureQueryResult, err error) {
	if mqo.TimeRange == nil {
		return nil, errors.New("invalid query options: timeRange are required")
	}
	if len(mqo.TagProjection) == 0 && len(mqo.FieldProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection or fieldProjection is required")
	}
	var tsdb storage.TSDB[*tsTable, option]
	db := m.tsdb.Load()
	if db == nil {
		tsdb, err = m.schemaRepo.loadTSDB(m.group)
		if err != nil {
			return nil, err
		}
		m.tsdb.Store(tsdb)
	} else {
		tsdb = db.(storage.TSDB[*tsTable, option])
	}

	segments, err := tsdb.SelectSegments(*mqo.TimeRange)
	if err != nil {
		return nil, err
	}
	if len(segments) < 1 {
		return nilResult, nil
	}

	if m.schema.IndexMode {
		return m.buildIndexQueryResult(ctx, mqo, segments)
	}

	if len(mqo.Entities) < 1 {
		return nil, errors.New("invalid query options: series is required")
	}

	series := make([]*pbv1.Series, len(mqo.Entities))
	for i := range mqo.Entities {
		series[i] = &pbv1.Series{
			Subject:      mqo.Name,
			EntityValues: mqo.Entities[i],
		}
	}

	sids, tables, _, _, storedIndexValue, newTagProjection, err := m.searchSeriesList(ctx, series, mqo, segments)
	if err != nil {
		return nil, err
	}
	if len(sids) < 1 {
		for i := range segments {
			segments[i].DecRef()
		}
		return nilResult, nil
	}
	result := queryResult{
		ctx:              ctx,
		segments:         segments,
		tagProjection:    mqo.TagProjection,
		storedIndexValue: storedIndexValue,
	}
	defer func() {
		if err != nil {
			result.Release()
		}
	}()
	mqo.TagProjection = newTagProjection
	var parts []*part
	qo := queryOptions{
		MeasureQueryOptions: mqo,
		minTimestamp:        mqo.TimeRange.Start.UnixNano(),
		maxTimestamp:        mqo.TimeRange.End.UnixNano(),
	}
	var n int
	for i := range tables {
		s := tables[i].currentSnapshot()
		if s == nil {
			continue
		}
		parts, n = s.getParts(parts, cache, qo.minTimestamp, qo.maxTimestamp)
		if n < 1 {
			s.decRef()
			continue
		}
		result.snapshots = append(result.snapshots, s)
	}

	if err = m.searchBlocks(ctx, &result, sids, parts, qo); err != nil {
		return nil, err
	}

	if mqo.Order == nil {
		result.ascTS = true
		result.orderByTS = true
	} else {
		if mqo.Order.Sort == modelv1.Sort_SORT_ASC || mqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.ascTS = true
		}
		switch mqo.Order.Type {
		case index.OrderByTypeTime:
			result.orderByTS = true
		case index.OrderByTypeIndex:
			result.orderByTS = false
		case index.OrderByTypeSeries:
			result.orderByTS = false
		}
	}

	return &result, nil
}

func BenchmarkMeasureQuery(b *testing.B) {
	b.ReportAllocs()
	ctx := context.TODO()

	for _, p := range pList {
		dpsList := generateData(p)
		db, cache, cleanup := write(b, p, dpsList)
		defer cleanup()
		m := generateMeasure(db, cache)
		mqo := generateMeasureQueryOptions(p)

		result, err := m.QueryWithCache(ctx, mqo, cache)
		require.NoError(b, err)
		if result != nil {
			result.Release()
		}

		b.Run("query-with-cache", func(b *testing.B) {
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					result, err := m.QueryWithCache(ctx, mqo, cache)
					require.NoError(b, err)
					if result != nil {
						result.Release()
					}
				}
			})
		})
	}
}
