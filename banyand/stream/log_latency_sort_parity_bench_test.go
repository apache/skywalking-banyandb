// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stream

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// The log shape under test: a service (the entity, so records group into
// series), a latency in milliseconds (the indexed sort key), a level derived
// from it, and a body. The query is the one an operator actually asks for --
// the slowest records for a set of services, ordered by latency descending.
const (
	logGroup    = "benchmark-logs"
	logStream   = "log_records"
	logFamily   = "log-family"
	logTagSvc   = "service"
	logTagLat   = "latency"
	logTagLevel = "level"
	logTagBody  = "body"

	logLatencyRuleID   = 11
	logLatencyRuleName = "latency-index"
)

// Canonical log volume: one million records across a hundred services, spread
// evenly over an hour. Override with LOGPARITY_RECORDS / LOGPARITY_SERVICES /
// LOGPARITY_PARTS to check other shapes.
var (
	logParityRecords  = envInt("LOGPARITY_RECORDS", 1_000_000)
	logParityServices = envInt("LOGPARITY_SERVICES", 100)
	logParityLimit    = envInt("LOGPARITY_LIMIT", 20)
	logParityParts    = envInt("LOGPARITY_PARTS", 50)
)

// logFixture is a stream holding log records, with latency indexed.
type logFixture struct {
	s       *stream
	tr      timestamp.TimeRange
	entries [][]*modelv1.TagValue
	rule    *databasev1.IndexRule
	tst     *tsTable
	sids    []common.SeriesID
	// latencyByID maps a document ID to the latency written with it, so an
	// index-ordered result can be compared with a merge-ordered one by value
	// instead of by internal identity.
	latencyByID map[uint64]int64
}

var logFixtures = map[string]*logFixture{}

// logLatency draws a latency in milliseconds.
//
// "random" is uniform over a log-plausible range. "tail" puts one percent of
// records far above the rest, so a descending top-N has to dig past a narrow
// extreme tail, which is where an index-ordered walk and a merge over
// everything differ most.
func logLatency(rng *rand.Rand, dist string, ordinal int) int64 {
	if dist == "tail" {
		if ordinal%100 == 0 {
			return int64(5_000 + rng.Intn(95_000))
		}
		return int64(1 + rng.Intn(300))
	}
	return int64(1 + rng.Intn(100_000))
}

// buildLogFixture writes logParityRecords log records as logParityParts storage
// segments and returns the fixture. It builds once per process per distribution.
func buildLogFixture(tb testing.TB, dist string) *logFixture {
	if f, ok := logFixtures[dist]; ok {
		return f
	}
	require.NoError(tb, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	// Fixed seed, so a parity failure is reproducible from the printed values.
	rng := rand.New(rand.NewSource(20261009))
	base := time.Now().Add(-2 * time.Hour).Truncate(time.Hour).UnixNano()

	sidByService := make([]common.SeriesID, logParityServices+1)
	entries := make([][]*modelv1.TagValue, 0, logParityServices)
	var entityDocs index.Documents
	for k := 1; k <= logParityServices; k++ {
		entity := []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(k)},
		}}}
		entries = append(entries, entity)
		series := &pbv1.Series{Subject: logStream, EntityValues: entity}
		require.NoError(tb, series.Marshal())
		sidByService[k] = series.ID
		entityDocs = append(entityDocs, index.Document{DocID: uint64(series.ID), EntityValues: series.Buffer})
	}

	dir, dirErr := os.MkdirTemp(os.Getenv("LOGPARITY_DIR"), "log-parity-")
	require.NoError(tb, dirErr)
	db := openDatabase(tb, dir)

	var minTS, maxTS int64
	perPart := logParityRecords / logParityParts
	if perPart == 0 {
		perPart = 1
	}
	latencyByID := make(map[uint64]int64, logParityRecords)
	var tst *tsTable

	for part := range logParityParts {
		elems := &elements{}
		var docs index.Documents
		for offset := range perPart {
			ordinal := part*perPart + offset
			service := (ordinal % logParityServices) + 1
			// Spread the hour evenly, one timestamp per record.
			ts := base + int64(ordinal)*(int64(time.Hour)/int64(logParityRecords))
			if minTS == 0 || ts < minTS {
				minTS = ts
			}
			if ts > maxTS {
				maxTS = ts
			}
			latency := logLatency(rng, dist, ordinal)
			level := "INFO"
			switch {
			case latency > 800:
				level = "ERROR"
			case latency > 300:
				level = "WARN"
			}
			eid := convert.HashStr(logStream + "-" + strconv.Itoa(ordinal))
			latencyByID[eid] = latency
			latencyText := strconv.FormatInt(latency, 10)
			body := "GET /api/v1/orders 200 " + latencyText + "ms"

			elems.seriesIDs = append(elems.seriesIDs, sidByService[service])
			elems.timestamps = append(elems.timestamps, ts)
			elems.elementIDs = append(elems.elementIDs, eid)
			elems.tagFamilies = append(elems.tagFamilies, []tagValues{{
				tag: logFamily,
				values: []*tagValue{
					{tag: logTagSvc, value: []byte(entityTagValuePrefix + strconv.Itoa(service)), valueType: pbv1.ValueTypeStr},
					{tag: logTagLat, value: []byte(latencyText), valueType: pbv1.ValueTypeStr},
					{tag: logTagLevel, value: []byte(level), valueType: pbv1.ValueTypeStr},
					{tag: logTagBody, value: []byte(body), valueType: pbv1.ValueTypeStr},
				},
			}})
			docs = append(docs, index.Document{
				DocID: eid, Timestamp: ts,
				Fields: []index.Field{index.NewBytesField(
					index.FieldKey{IndexRuleID: logLatencyRuleID, SeriesID: sidByService[service]},
					[]byte(latencyText))},
			})
		}
		segment, segErr := db.CreateSegmentIfNotExist(time.Unix(0, elems.timestamps[0]))
		require.NoError(tb, segErr)
		if part == 0 {
			require.NoError(tb, segment.IndexDB().Insert(entityDocs))
		}
		table, tableErr := segment.CreateTSTableIfNotExist(common.ShardID(0))
		require.NoError(tb, tableErr)
		tst = table
		tst.mustAddElements(elems)
		require.NoError(tb, tst.Index().Write(docs))
		segment.DecRef()
	}

	// Wait until every part is flushed to disk, so both paths read from files
	// rather than one of them reading a warm memtable.
	deadline := time.Now().Add(10 * time.Minute)
	for {
		snapshot := tst.currentSnapshot()
		live := 0
		for _, part := range snapshot.parts {
			if part.mp != nil {
				live++
			}
		}
		snapshot.decRef()
		if live == 0 {
			break
		}
		require.True(tb, time.Now().Before(deadline), "flush timeout: %d parts still in memory", live)
		time.Sleep(200 * time.Millisecond)
	}

	schema := &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: logStream, Group: logGroup},
		Entity:   &databasev1.Entity{TagNames: []string{logTagSvc}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: logFamily,
			Tags: []*databasev1.TagSpec{
				{Name: logTagSvc, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: logTagLat, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: logTagLevel, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: logTagBody, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}
	vectorized := vstream.DefaultConfig()
	vectorized.Enabled = true
	streamer := &stream{
		schema: schema,
		// name and group are normally set by New; a literal stream has to carry
		// them or a query resolves its TSDB under an empty group name.
		name: logStream, group: logGroup,
		l: logger.GetLogger("logparity"), pm: protector.Nop{},
		schemaRepo: newTestSchemaRepo(db, logGroup), vectorized: vectorized,
	}

	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: logLatencyRuleID, Name: logLatencyRuleName, Group: logGroup},
		Tags:     []string{logTagLat}, Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	// The vectorized scan reads index rules off this, which New would normally
	// populate from the schema repo's index-rule watcher.
	streamer.OnIndexUpdate([]*databasev1.IndexRule{rule})

	sids := make([]common.SeriesID, 0, logParityServices)
	for k := 1; k <= logParityServices; k++ {
		sids = append(sids, sidByService[k])
	}
	f := &logFixture{
		s: streamer, tr: timestamp.NewInclusiveTimeRange(time.Unix(0, minTS), time.Unix(0, maxTS)),
		entries: entries,
		rule:    rule,
		tst:     tst, sids: sids, latencyByID: latencyByID,
	}
	logFixtures[dist] = f
	return f
}

func (f *logFixture) sqo() model.StreamQueryOptions {
	return model.StreamQueryOptions{
		Name: logStream, TimeRange: &f.tr, Entities: f.entries,
		TagProjection: []model.TagProjection{{
			Family: logFamily, Names: []string{logTagSvc, logTagLat, logTagLevel, logTagBody},
		}},
		Order:          &index.OrderBy{Index: f.rule, Sort: modelv1.Sort_SORT_DESC},
		MaxElementSize: logParityLimit,
	}
}

// indexSortedLatency returns the top-N latencies through the element index's
// own sorted walk, loading no element block at all.
func (f *logFixture) indexSortedLatency(ctx context.Context) []int64 {
	iterator, sortErr := f.tst.Index().Sort(ctx, f.sids,
		index.FieldKey{IndexRuleID: logLatencyRuleID}, modelv1.Sort_SORT_DESC, &f.tr, logParityLimit)
	if sortErr != nil {
		panic(sortErr)
	}
	defer func() { _ = iterator.Close() }()
	seen := make(map[uint64]struct{}, logParityLimit)
	out := make([]int64, 0, logParityLimit)
	for len(out) < logParityLimit && iterator.Next() {
		id := iterator.Val().DocID
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, f.latencyByID[id])
	}
	return out
}

// mergeSortedLatency returns the top-N latencies through the vectorized merge
// pipeline, which is what a real query uses today.
func (f *logFixture) mergeSortedLatency(ctx context.Context, t *testing.T) []int64 {
	elements := runVecPipeline(ctx, t, f.s, f.sqo(), true, uint32(logParityLimit))
	require.Len(t, elements, logParityLimit)
	out := make([]int64, 0, len(elements))
	for _, element := range elements {
		out = append(out, elementLatency(element))
	}
	return out
}

func elementLatency(element *streamv1.Element) int64 {
	for _, family := range element.TagFamilies {
		for _, tag := range family.Tags {
			if tag.Key != logTagLat {
				continue
			}
			if parsed, err := strconv.ParseInt(tag.Value.GetStr().GetValue(), 10, 64); err == nil {
				return parsed
			}
		}
	}
	return 0
}

// TestLogLatencySortParity is the correctness half: the index-ordered walk and
// the merge-ordered pipeline must return the same records in the same order.
// They are only comparable if they agree first.
func TestLogLatencySortParity(t *testing.T) {
	ctx := context.Background()
	for _, dist := range []string{"random", "tail"} {
		f := buildLogFixture(t, dist)
		fromIndex := f.indexSortedLatency(ctx)
		fromMerge := f.mergeSortedLatency(ctx, t)
		require.Equal(t, fromIndex, fromMerge,
			"dist=%s: index-ordered and merge-ordered top-%d latencies differ\nindex: %v\nmerge: %v",
			dist, logParityLimit, fromIndex, fromMerge)
		t.Logf("dist=%s records=%d services=%d parts=%d top latency=%d ms",
			dist, logParityRecords, logParityServices, logParityParts, fromMerge[0])
	}
}

// BenchmarkLogLatencySortParity measures the two ways a stream query can answer
// "the slowest records".
//
//	index-based walks the latency term dictionary through the element index,
//	           touching no element block at all;
//	memory-based materialises batches and merges them in sort order, which is
//	           what ExecuteVectorized does today.
//
// TestLogLatencySortParity establishes that the two return the same records.
func BenchmarkLogLatencySortParity(b *testing.B) {
	ctx := context.Background()
	for _, dist := range []string{"random", "tail"} {
		f := buildLogFixture(b, dist)
		b.Run(dist+"/index-based", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				f.indexSortedLatency(ctx)
			}
		})
		b.Run(dist+"/memory-based", func(b *testing.B) {
			probe := &testing.T{}
			b.ReportAllocs()
			for range b.N {
				f.mergeSortedLatency(ctx, probe)
			}
		})
	}
}
