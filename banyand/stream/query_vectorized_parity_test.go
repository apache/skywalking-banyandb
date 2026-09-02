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
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

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
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	indexOrderRuleID      = 1
	indexOrderSeriesCount = 4
	indexOrderTSCount     = 12
	entityTagName         = "entity-tag"
)

// parityFixture is a golden expectation file under testdata/. The files were
// captured from the row query path before it was removed (apache/skywalking#13998)
// and cannot be regenerated; they are the frozen oracle these tests assert against.
type parityFixture struct {
	Name  string       `yaml:"name"`
	Cases []parityCase `yaml:"cases"`
}

// parityCase is one query shape within a golden file.
type parityCase struct {
	Name         string          `yaml:"name"`
	Query        string          `yaml:"query"`
	Elements     []parityElement `yaml:"elements"`
	ElementCount int             `yaml:"element_count"`
}

// parityElement is one expected element. TS is a nanosecond OFFSET from the query
// time range start, because the fixture writer anchors element timestamps at
// time.Now(); only entity-tag is recorded because the sibling filter-tag is drawn
// from crypto/rand per run and no golden can pin it.
type parityElement struct {
	ID        string `yaml:"id"`
	EntityTag string `yaml:"entity-tag"`
	TS        int64  `yaml:"ts"`
}

// loadParityGolden reads a golden file from testdata/ and indexes its cases by name.
func loadParityGolden(t *testing.T, name string) map[string]parityCase {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name+".yaml"))
	require.NoError(t, err)
	var fixture parityFixture
	require.NoError(t, yaml.Unmarshal(raw, &fixture))
	require.Equal(t, name, fixture.Name, "golden file name does not match its content")
	byName := make(map[string]parityCase, len(fixture.Cases))
	for _, c := range fixture.Cases {
		require.Len(t, c.Elements, c.ElementCount, "golden case %q is self-inconsistent", c.Name)
		byName[c.Name] = c
	}
	return byName
}

// entityTagValue returns the element's entity-tag value.
func entityTagValue(t *testing.T, e *streamv1.Element) string {
	t.Helper()
	for _, family := range e.TagFamilies {
		for _, tag := range family.Tags {
			if tag.Key == entityTagName {
				return tag.Value.GetStr().GetValue()
			}
		}
	}
	t.Fatalf("element %s carries no %s", e.ElementId, entityTagName)
	return ""
}

// assertGoldenParity asserts the vec elements match the golden case: the same
// element-id set, each with the recorded timestamp offset from trStart and the
// recorded entity-tag. exact additionally pins the ORDER, which only holds for
// fixtures with globally unique timestamps (seriesCount=1); multi-series fixtures
// stamp duplicate timestamps whose relative order is not path-stable, so those
// cases assert the set plus ts monotonicity instead.
func assertGoldenParity(t *testing.T, want parityCase, got []*streamv1.Element, trStart time.Time, exact, desc bool) {
	t.Helper()
	// A missing map key yields a zero parityCase, which would make an empty-result
	// assertion pass vacuously; Name is only set when the lookup actually hit.
	require.NotEmpty(t, want.Name, "golden case not found in its fixture")
	require.Len(t, got, want.ElementCount, "case %q: element count mismatch", want.Name)
	gotByID := indexByElementID(t, got)
	for idx, wantElem := range want.Elements {
		gotElem, ok := gotByID[wantElem.ID]
		require.True(t, ok, "case %q: vec path missing golden elementID %s", want.Name, wantElem.ID)
		require.Equal(t, wantElem.TS, gotElem.Timestamp.AsTime().UnixNano()-trStart.UnixNano(),
			"case %q: ts offset mismatch for %s", want.Name, wantElem.ID)
		require.Equal(t, wantElem.EntityTag, entityTagValue(t, gotElem),
			"case %q: entity-tag mismatch for %s", want.Name, wantElem.ID)
		if exact {
			require.Equal(t, wantElem.ID, got[idx].ElementId, "case %q: element order mismatch at %d", want.Name, idx)
		}
	}
	assertMonotonicTS(t, got, desc)
}

// parityEntities builds the per-series entity tag-value lists the fixture writer
// keys on (entity1..entityN), matching the entity index docs writeVecFixture adds.
func parityEntities(seriesCount int) [][]*modelv1.TagValue {
	entities := make([][]*modelv1.TagValue, 0, seriesCount)
	for i := 1; i <= seriesCount; i++ {
		entities = append(entities, []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(i)}}},
		})
	}
	return entities
}

// fullProjection is the two-tag projection the fixture populates on every element.
func fullProjection() []model.TagProjection {
	return []model.TagProjection{{
		Family: "benchmark-family",
		Names:  []string{entityTagName, "filter-tag"},
	}}
}

// runVecPipeline drives the vec scan through the FULL M4 pipeline
// (merge → distinct → limit) exactly as localIndexScan.ExecuteVectorized does at
// the data-node standalone path, then materializes []*streamv1.Element via the
// batch egress. limitRows is the M4 pipeline cap; pass 0 for "no cap" (drain
// everything).
func runVecPipeline(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions,
	desc bool, limitRows uint32,
) []*streamv1.Element {
	t.Helper()
	src, err := s.queryVectorized(ctx, sqo)
	require.NoError(t, err)
	require.NotNil(t, src)
	schema := src.Schema()
	// Mirror ExecuteVectorized: the merge caps at maxElementSize (in-order top-N),
	// and the trailing Limit applies the client offset/limit slice.
	mergeCap := 0
	if sqo.MaxElementSize > 0 && sqo.MaxElementSize < math.MaxInt32 {
		mergeCap = sqo.MaxElementSize
	}
	pipeline, err := vstream.BuildStreamMergePipeline(
		&testVecSource{src: src, schema: schema}, schema, desc, 0, limitRows, vstream.DefaultConfig().BatchSize, mergeCap)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	var batches []*vectorized.RecordBatch
	for {
		batch, nextErr := pipeline.Next(ctx)
		require.NoError(t, nextErr)
		if batch == nil {
			break
		}
		batches = append(batches, batch)
	}
	elems, err := BuildElementsFromBatches(batches, sqo.TagProjection)
	require.NoError(t, err)
	require.NoError(t, pipeline.Close())
	return elems
}

// assertMonotonicTS asserts the elements are monotonically ordered by timestamp
// (non-decreasing for asc, non-increasing for desc).
func assertMonotonicTS(t *testing.T, elems []*streamv1.Element, desc bool) {
	t.Helper()
	for i := 1; i < len(elems); i++ {
		prev := elems[i-1].Timestamp.AsTime().UnixNano()
		cur := elems[i].Timestamp.AsTime().UnixNano()
		if desc {
			require.GreaterOrEqual(t, prev, cur, "output not desc-ordered by ts at %d", i)
		} else {
			require.LessOrEqual(t, prev, cur, "output not asc-ordered by ts at %d", i)
		}
	}
}

// TestQueryVectorized_Parity_Boundary covers the degenerate result shapes: empty
// (query window with no data), single-element, and a >=2-block fixture so
// cross-block ordering is exercised, against the parity_boundary golden. These
// shapes sit below the server layer (maxElementSize caps, physical block splits)
// and are unreachable by the server-level fixtures in test/cases/stream/data/want.
func TestQueryVectorized_Parity_Boundary(t *testing.T) {
	golden := loadParityGolden(t, "parity_boundary")

	t.Run("empty-result", func(t *testing.T) {
		p := parameter{
			batchCount:     1,
			timestampCount: 20,
			seriesCount:    5,
			tagCardinality: 4,
			startTimestamp: 1,
			endTimestamp:   1000,
			scenario:       "vec-parity-empty",
		}
		s, tr := buildVecTestStream(t, p)
		entities := parityEntities(p.seriesCount)
		ctx := context.Background()

		// A time range entirely BEFORE the fixture window (which starts at ~now-1h):
		// SelectSegments still returns the recent segment but the scan yields no rows
		// in [tr.Start, tr.Start] shifted 100 years back, so the result is empty.
		emptyStart := tr.Start.Add(-100 * 365 * 24 * time.Hour)
		emptyTR := timestamp.NewInclusiveTimeRange(emptyStart, tr.Start.Add(-time.Nanosecond))
		sqo := model.StreamQueryOptions{
			Name:           "benchmark",
			TimeRange:      &emptyTR,
			Entities:       entities,
			TagProjection:  fullProjection(),
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: math.MaxInt32,
		}
		// Pass a real (non-zero) limit so the emptiness comes from the no-data window,
		// not from Limit(0) emitting nothing by construction (which would make this a
		// tautology). A large limit lets any wrongly-returned row through and fail.
		vecElements := runVecPipeline(ctx, t, s, sqo, false, math.MaxUint32)
		assertGoldenParity(t, golden["empty-result"], vecElements, emptyTR.Start, true, false)
	})

	t.Run("single-element", func(t *testing.T) {
		// seriesCount=1, timestampCount=1 => exactly one element.
		p := parameter{
			batchCount:     1,
			timestampCount: 1,
			seriesCount:    1,
			tagCardinality: 4,
			startTimestamp: 1,
			endTimestamp:   1000,
			scenario:       "vec-parity-single",
		}
		s, tr := buildVecTestStream(t, p)
		entities := parityEntities(p.seriesCount)
		ctx := context.Background()
		sqo := model.StreamQueryOptions{
			Name:           "benchmark",
			TimeRange:      &tr,
			Entities:       entities,
			TagProjection:  fullProjection(),
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: math.MaxInt32,
		}
		vecElements := runVecPipeline(ctx, t, s, sqo, false, 4)
		assertGoldenParity(t, golden["single-element"], vecElements, tr.Start, true, false)
	})

	t.Run("multi-block-cross-block-order", func(t *testing.T) {
		// A block splits on series-id change (part.go:204), so seriesCount=3 forces
		// >=3 physical blocks within the single part; the vec SortedMerge must order
		// across those blocks. Multiple series share timestamps (ties), so the golden
		// comparison is set-based + ts-monotonic rather than order-exact.
		p := parameter{
			batchCount:     1,
			timestampCount: 50,
			seriesCount:    3,
			tagCardinality: 4,
			startTimestamp: 1,
			endTimestamp:   1000,
			scenario:       "vec-parity-multiblock",
		}
		s, tr := buildVecTestStream(t, p)
		entities := parityEntities(p.seriesCount)
		ctx := context.Background()
		sqo := model.StreamQueryOptions{
			Name:           "benchmark",
			TimeRange:      &tr,
			Entities:       entities,
			TagProjection:  fullProjection(),
			Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
			MaxElementSize: math.MaxInt32,
		}
		ascCase := golden["multi-block-cross-block-order-asc"]
		vecElements := runVecPipeline(ctx, t, s, sqo, false, uint32(ascCase.ElementCount*4))
		assertGoldenParity(t, ascCase, vecElements, tr.Start, false, false)

		// Same fixture, DESC, to exercise the desc cross-block merge direction.
		sqoDesc := sqo
		sqoDesc.Order = &index.OrderBy{Sort: modelv1.Sort_SORT_DESC}
		descCase := golden["multi-block-cross-block-order-desc"]
		vecDesc := runVecPipeline(ctx, t, s, sqoDesc, true, uint32(descCase.ElementCount*4))
		assertGoldenParity(t, descCase, vecDesc, tr.Start, false, true)
	})
}

// TestQueryVectorized_Parity_LargeSingleBlock is the C1 regression: a single
// block with MORE than DefaultBatchSize (1024) rows. Stream blocks are capped by
// uncompressed BYTES (2 MiB), not row count, so a block can hold >1024 rows; the
// scan must drain a cursor across multiple batchSize batches. Before the fix the
// scan emitted only the first batchSize rows of a cursor and silently dropped the
// rest, so the result would be a strict subset of the golden. seriesCount=1 =>
// one series => one physical block => globally unique timestamps => the golden
// pins the exact order.
func TestQueryVectorized_Parity_LargeSingleBlock(t *testing.T) {
	const rows = 1500 // > DefaultBatchSize (1024), single series => single block.
	p := parameter{
		batchCount:     1,
		timestampCount: rows,
		seriesCount:    1,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   10000,
		scenario:       "vec-parity-large-single-block",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: math.MaxInt32,
	}
	vecElements := runVecPipeline(ctx, t, s, sqo, false, uint32(rows*4))
	assertGoldenParity(t, loadParityGolden(t, "parity_large_single_block")["asc"], vecElements, tr.Start, true, false)
}

// TestQueryVectorized_Parity_TopN_Desc is the C2 regression for descending
// top-N: ORDER BY ts DESC with MaxElementSize < total must return the NEWEST N
// rows, not the oldest N. Before the fix the scan truncated the first N rows in
// storage (ascending) order before the merge, so a desc query returned the
// OLDEST N. seriesCount=1 => unique timestamps => the golden pins the exact order.
func TestQueryVectorized_Parity_TopN_Desc(t *testing.T) {
	const total = 100
	const topN = 10
	p := parameter{
		batchCount:     1,
		timestampCount: total,
		seriesCount:    1,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   10000,
		scenario:       "vec-parity-topn-desc",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_DESC},
		MaxElementSize: topN, // per-node cap < total => in-order top-N.
	}
	// The vec merge caps at MaxElementSize (topN) in DESC sort order; no client
	// offset/limit slice (limit=topN, offset=0) so egress keeps all N.
	vecElements := runVecPipeline(ctx, t, s, sqo, true, uint32(topN))
	assertGoldenParity(t, loadParityGolden(t, "parity_topn_desc")["desc"], vecElements, tr.Start, true, true)
}

// TestQueryVectorized_Parity_TopN_MultiSeries_Asc is the C2 cross-series
// regression: an ASC query over MULTIPLE series with MaxElementSize < total.
// Blocks are seriesID-major, so a pre-merge storage-order cap would keep rows
// from the first series(es) only, not the globally-oldest N across all series.
// The cap must apply AFTER the cross-series merge (in ts order). The five series
// share timestamps, so the boundary timestamp has ties and the kept element-id set
// at that boundary is not path-stable; the golden's invariant is therefore the
// KEPT TS WINDOW (its largest offset), not the exact id set.
func TestQueryVectorized_Parity_TopN_MultiSeries_Asc(t *testing.T) {
	p := parameter{
		batchCount:     1,
		timestampCount: 40,
		seriesCount:    5,
		tagCardinality: 4,
		startTimestamp: 1,
		endTimestamp:   10000,
		scenario:       "vec-parity-topn-multiseries",
	}
	s, tr := buildVecTestStream(t, p)
	entities := parityEntities(p.seriesCount)
	ctx := context.Background()
	const topN = 30 // < 40*5 = 200 total.
	sqo := model.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &tr,
		Entities:       entities,
		TagProjection:  fullProjection(),
		Order:          &index.OrderBy{Sort: modelv1.Sort_SORT_ASC},
		MaxElementSize: topN,
	}
	want := loadParityGolden(t, "parity_topn_multiseries_asc")["asc"]
	vecElements := runVecPipeline(ctx, t, s, sqo, false, uint32(topN))
	require.Len(t, vecElements, want.ElementCount, "vec must keep exactly the in-order top-N across series")
	require.Equal(t, maxGoldenTS(want), maxTS(vecElements)-tr.Start.UnixNano(),
		"vec kept a different ts window than the golden (wrong cross-series top-N)")
	assertMonotonicTS(t, vecElements, false)
}

// maxTS returns the largest timestamp (UnixNano) across the elements.
func maxTS(elems []*streamv1.Element) int64 {
	var m int64
	for _, e := range elems {
		if ts := e.Timestamp.AsTime().UnixNano(); ts > m {
			m = ts
		}
	}
	return m
}

// maxGoldenTS returns the largest recorded timestamp offset in a golden case.
func maxGoldenTS(c parityCase) int64 {
	var m int64
	for _, e := range c.Elements {
		if e.TS > m {
			m = e.TS
		}
	}
	return m
}

// buildIndexOrderStream builds a stream over a DEDICATED index-order fixture whose
// sortable index docs carry DocID=elementID and Timestamp=ts, and whose element
// "filter-tag" value equals the sortable field's term so the vec OrderKey path
// (resolveOrderTag) can key on it. The shared generateData fixture cannot serve
// here: it stamps DocID=timestamp and sets no Timestamp on its docs.
//
// Sort values are assigned so index order deliberately DIFFERS from ts order (the
// per-series value decreases as ts increases), proving the test exercises the
// order-key path rather than accidentally matching timestamp order.
func buildIndexOrderStream(t *testing.T) (*stream, timestamp.TimeRange) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))
	base := time.Now().Add(-time.Hour).Truncate(time.Hour)

	elems := &elements{}
	var sortDocs index.Documents
	var minTS, maxTS int64
	// Precompute hashed series ids the entity index derives, so the block scanner's
	// index-resolved series id matches the element rows and the sort field key.
	sidByK := make(map[int]common.SeriesID, indexOrderSeriesCount)
	var entityDocs index.Documents
	for k := 1; k <= indexOrderSeriesCount; k++ {
		entity := []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityTagValuePrefix + strconv.Itoa(k)}}},
		}
		series := &pbv1.Series{Subject: "benchmark", EntityValues: entity}
		require.NoError(t, series.Marshal())
		sidByK[k] = series.ID
		entityDocs = append(entityDocs, index.Document{DocID: uint64(series.ID), EntityValues: series.Buffer})
	}

	for k := 1; k <= indexOrderSeriesCount; k++ {
		sid := sidByK[k]
		for j := 1; j <= indexOrderTSCount; j++ {
			ts := base.UnixNano() + int64(j)*int64(time.Second)
			if minTS == 0 || ts < minTS {
				minTS = ts
			}
			if ts > maxTS {
				maxTS = ts
			}
			elementID := convert.HashStr(strconv.Itoa(k) + "-" + strconv.Itoa(j))
			// Sort value decreases as ts increases => index order != ts order. Zero-pad
			// so lexicographic term order equals numeric order.
			sortValue := filterTagValuePrefix + fmt.Sprintf("%03d", indexOrderTSCount-j)

			elems.seriesIDs = append(elems.seriesIDs, sid)
			elems.timestamps = append(elems.timestamps, ts)
			elems.elementIDs = append(elems.elementIDs, elementID)
			elems.tagFamilies = append(elems.tagFamilies, []tagValues{{
				tag: "benchmark-family",
				values: []*tagValue{
					{tag: entityTagName, value: []byte(entityTagValuePrefix + strconv.Itoa(k)), valueType: pbv1.ValueTypeStr},
					{tag: "filter-tag", value: []byte(sortValue), valueType: pbv1.ValueTypeStr},
				},
			}})

			sortDocs = append(sortDocs, index.Document{
				DocID:     elementID,
				Timestamp: ts,
				Fields: []index.Field{
					index.NewBytesField(index.FieldKey{IndexRuleID: indexOrderRuleID, SeriesID: sid}, []byte(sortValue)),
				},
			})
		}
	}

	db := openDatabase(t, t.TempDir())
	t.Cleanup(func() { _ = db.Close() })
	seg, err := db.CreateSegmentIfNotExist(time.Unix(0, elems.timestamps[0]))
	require.NoError(t, err)
	seg.IndexDB().Insert(entityDocs)
	tst, err := seg.CreateTSTableIfNotExist(common.ShardID(0))
	require.NoError(t, err)
	tst.mustAddElements(elems)
	tst.Index().Write(sortDocs)
	seg.DecRef()

	entity := &databasev1.Entity{TagNames: []string{entityTagName}}
	tagFamily := &databasev1.TagFamilySpec{
		Name: "benchmark-family",
		Tags: []*databasev1.TagSpec{
			{Name: entityTagName, Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "filter-tag", Type: databasev1.TagType_TAG_TYPE_STRING},
		},
	}
	schema := &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Name: "benchmark", Group: "test"},
		Entity:      entity,
		TagFamilies: []*databasev1.TagFamilySpec{tagFamily},
	}
	s := &stream{
		schema:     schema,
		l:          logger.GetLogger("test-vec-idxorder"),
		pm:         protector.Nop{},
		vectorized: vstream.DefaultConfig(),
	}
	s.name, s.group = "benchmark", "test"
	var is indexSchema
	is.parse(schema)
	s.indexSchema = atomic.Value{}
	s.indexSchema.Store(is)
	s.tsdb.Store(db)
	return s, timestamp.NewInclusiveTimeRange(time.Unix(0, minTS), time.Unix(0, maxTS))
}

// runVecHiddenOrderTag mirrors localIndexScan.ExecuteVectorized for an index-order
// query whose ordered tag is NOT in the client projection: the scan is asked for
// scanProjection (client tags + the ordered tag) so the OrderKey column can be
// populated, and the egress materializes only clientProjection, so the ordered tag
// never reaches the result.
func runVecHiddenOrderTag(ctx context.Context, t *testing.T, s *stream, sqo model.StreamQueryOptions,
	scanProjection, clientProjection []model.TagProjection, desc bool,
) []*streamv1.Element {
	t.Helper()
	scanSQO := sqo
	scanSQO.TagProjection = scanProjection
	src, err := s.queryVectorized(ctx, scanSQO)
	require.NoError(t, err)
	require.NotNil(t, src)
	schema := src.Schema()
	pipeline, err := vstream.BuildStreamMergePipeline(
		&testVecSource{src: src, schema: schema}, schema, desc, 0, math.MaxUint32, vstream.DefaultConfig().BatchSize, 0)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(ctx))
	var batches []*vectorized.RecordBatch
	for {
		batch, nextErr := pipeline.Next(ctx)
		require.NoError(t, nextErr)
		if batch == nil {
			break
		}
		batches = append(batches, batch)
	}
	elems, err := BuildElementsFromBatches(batches, clientProjection)
	require.NoError(t, err)
	require.NoError(t, pipeline.Close())
	return elems
}

// TestQueryVectorized_Parity_IndexOrder_TagNotProjected covers the R-1 gap: an
// index-order query whose ordered tag is absent from the projection. Vec derives
// its OrderKey from the ordered tag's projected cell, so such a query used to fall
// back to the row path; it now projects the tag internally and strips it before
// egress.
//
// The expectation is analytic rather than captured: buildIndexOrderStream assigns
// each element j (1..indexOrderTSCount) the sort term indexOrderTSCount-j and the
// timestamp base+j seconds, so index-asc order is j descending — the exact REVERSE
// of timestamp order. A silent timestamp fallback, the failure this closes, would
// produce the opposite sequence and fail.
func TestQueryVectorized_Parity_IndexOrder_TagNotProjected(t *testing.T) {
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "filter-idx", Id: indexOrderRuleID},
		Tags:     []string{"filter-tag"},
	}
	s, tr := buildIndexOrderStream(t)
	ctx := context.Background()
	// One series, so the order key has no cross-series ties and the total order is
	// fully determined by the fixture.
	entities := parityEntities(1)
	clientProjection := []model.TagProjection{{Family: "benchmark-family", Names: []string{entityTagName}}}
	scanProjection := []model.TagProjection{{Family: "benchmark-family", Names: []string{entityTagName, "filter-tag"}}}

	for _, tc := range []struct {
		name string
		sort modelv1.Sort
	}{
		{name: "index-order-asc", sort: modelv1.Sort_SORT_ASC},
		{name: "index-order-desc", sort: modelv1.Sort_SORT_DESC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desc := tc.sort == modelv1.Sort_SORT_DESC
			sqo := model.StreamQueryOptions{
				Name:           "benchmark",
				TimeRange:      &tr,
				Entities:       entities,
				TagProjection:  clientProjection,
				Order:          &index.OrderBy{Index: indexRule, Sort: tc.sort},
				MaxElementSize: math.MaxInt32,
			}
			vecElements := runVecHiddenOrderTag(ctx, t, s, sqo, scanProjection, clientProjection, desc)
			require.Len(t, vecElements, indexOrderTSCount, "index-sort produced an unexpected element count")

			for idx, e := range vecElements {
				// index-asc walks j from indexOrderTSCount down to 1; index-desc walks it up.
				j := indexOrderTSCount - idx
				if desc {
					j = idx + 1
				}
				wantID := hex.EncodeToString(convert.Uint64ToBytes(convert.HashStr("1-" + strconv.Itoa(j))))
				require.Equal(t, wantID, e.ElementId, "element order mismatch at %d", idx)
				require.Len(t, e.TagFamilies, 1)
				require.Len(t, e.TagFamilies[0].Tags, 1, "the ordered tag must not reach the egress")
				require.Equal(t, entityTagName, e.TagFamilies[0].Tags[0].Key)
			}
			// The fixture's sort value decreases as ts increases, so index order is
			// the reverse of ts order — a timestamp fallback would fail here.
			assertMonotonicTS(t, vecElements, !desc)
		})
	}
}
