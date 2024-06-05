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
	"crypto/rand"
	"io"
	"math"
	"math/big"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	logicalstream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	segmentMetadataFilename = "metadata"
	version                 = "1.0.0"
	entityTagValuePrefix    = "entity"
	filterTagValuePrefix    = "value"
)

type parameter struct {
	scenario       string
	batchCount     int
	timestampCount int
	seriesCount    int
	tagCardinality int
	startTimestamp int
	endTimestamp   int
}

var pList = [3]parameter{
	{batchCount: 2, timestampCount: 500, seriesCount: 100, tagCardinality: 10, startTimestamp: 1, endTimestamp: 1000, scenario: "large-scale"},
	{batchCount: 2, timestampCount: 500, seriesCount: 100, tagCardinality: 10, startTimestamp: 900, endTimestamp: 1000, scenario: "latest"},
	{batchCount: 2, timestampCount: 500, seriesCount: 100, tagCardinality: 10, startTimestamp: 300, endTimestamp: 400, scenario: "historical"},
}

type mockIndex map[string]map[common.SeriesID]posting.List

func (mi mockIndex) insert(value string, seriesID common.SeriesID, timestamp int) {
	if _, ok := mi[value]; !ok {
		mi[value] = make(map[common.SeriesID]posting.List)
	}
	if _, ok := mi[value][seriesID]; !ok {
		mi[value][seriesID] = roaring.NewPostingList()
	}
	mi[value][seriesID].Insert(uint64(timestamp))
}

type mockFilter struct {
	index mockIndex
	value string
}

func (mf mockFilter) String() string {
	return "filter"
}

func (mf mockFilter) Execute(_ index.GetSearcher, seriesID common.SeriesID) (posting.List, error) {
	return mf.index[mf.value][seriesID], nil
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

func generateRandomNumber(max int64) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(max))
	return int(n.Int64()) + 1
}

func generateData(p parameter) ([]*elements, []index.Documents, mockIndex) {
	esList := make([]*elements, 0)
	docsList := make([]index.Documents, 0)
	idx := make(mockIndex)
	for i := 0; i < p.batchCount; i++ {
		es := &elements{
			seriesIDs:   []common.SeriesID{},
			timestamps:  []int64{},
			elementIDs:  []string{},
			tagFamilies: [][]tagValues{},
		}
		var docs index.Documents
		for j := 1; j <= p.timestampCount; j++ {
			timestamp := i*p.timestampCount + j
			unixTimestamp := time.Unix(int64(timestamp), 0).UnixNano()
			for k := 1; k <= p.seriesCount; k++ {
				elementID := strconv.Itoa(k) + strconv.Itoa(timestamp)
				es.seriesIDs = append(es.seriesIDs, common.SeriesID(k))
				es.elementIDs = append(es.elementIDs, elementID)
				es.timestamps = append(es.timestamps, unixTimestamp)
				num := generateRandomNumber(int64(p.tagCardinality))
				value := filterTagValuePrefix + strconv.Itoa(num)
				tf := tagValues{
					tag: "benchmark-family",
					values: []*tagValue{{
						tag:       "entity-tag",
						value:     []byte(entityTagValuePrefix + strconv.Itoa(k)),
						valueType: pbv1.ValueTypeStr,
					}, {
						tag:       "filter-tag",
						value:     []byte(value),
						valueType: pbv1.ValueTypeStr,
					}},
				}
				tfs := []tagValues{tf}
				es.tagFamilies = append(es.tagFamilies, tfs)
				idx.insert(value, common.SeriesID(k), int(unixTimestamp))
				var fields []index.Field
				fields = append(fields, index.Field{
					Key: index.FieldKey{
						IndexRuleID: 1,
						SeriesID:    common.SeriesID(k),
					},
					Term: []byte(value),
				})
				docs = append(docs, index.Document{
					DocID:  uint64(unixTimestamp),
					Fields: fields,
				})
			}
		}
		esList = append(esList, es)
		docsList = append(docsList, docs)
	}
	return esList, docsList, idx
}

func openDatabase(b *testing.B, path string) storage.TSDB[*tsTable, option] {
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
	}
	db, err := storage.OpenTSDB(
		common.SetPosition(context.Background(), func(p common.Position) common.Position {
			p.Module = "stream"
			p.Database = "benchmark"
			return p
		}),
		opts)
	require.NoError(b, err)
	return db
}

func write(b *testing.B, p parameter, esList []*elements, docsList []index.Documents) storage.TSDB[*tsTable, option] {
	// Initialize a tstIter object.
	tmpPath, defFn := test.Space(require.New(b))
	segmentPath := filepath.Join(tmpPath, "shard-0", "seg-19700101")
	fileSystem := fs.NewLocalFileSystem()
	defer defFn()
	tst, err := newTSTable(fileSystem, segmentPath, common.Position{},
		// Since Stream deduplicate data in merging process, we need to disable the merging in the test.
		logger.GetLogger("benchmark"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDisabledMergePolicyForTesting()})
	require.NoError(b, err)
	for i := 0; i < len(esList); i++ {
		tst.mustAddElements(esList[i])
		tst.index.Write(docsList[i])
		time.Sleep(100 * time.Millisecond)
	}
	// wait until the introducer is done
	if len(esList) > 0 {
		for {
			snp := tst.currentSnapshot()
			if snp == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if snp.creator != snapshotCreatorMemPart && len(snp.parts) == len(esList) {
				snp.decRef()
				tst.Close()
				break
			}
			snp.decRef()
			time.Sleep(100 * time.Millisecond)
		}
	}
	data := []byte(version)
	metadataPath := filepath.Join(segmentPath, segmentMetadataFilename)
	lf, err := fileSystem.CreateLockFile(metadataPath, filePermission)
	require.NoError(b, err)
	_, err = lf.Write(data)
	require.NoError(b, err)
	db := openDatabase(b, tmpPath)
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
		err = series.Marshal()
		require.NoError(b, err)
		docs = append(docs, index.Document{
			DocID:        uint64(i),
			EntityValues: series.Buffer,
		})
		db.IndexDB().Write(docs)
	}
	return db
}

func generateStream(db storage.TSDB[*tsTable, option]) *stream {
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
				Name:        "entity-tag",
				Type:        databasev1.TagType_TAG_TYPE_STRING,
				IndexedOnly: false,
			},
			{
				Name:        "filter-tag",
				Type:        databasev1.TagType_TAG_TYPE_STRING,
				IndexedOnly: false,
			},
		},
	}
	schema := &databasev1.Stream{
		Entity:      entity,
		TagFamilies: []*databasev1.TagFamilySpec{tagFamily},
	}
	return &stream{
		databaseSupplier: dbSupplier,
		schema:           schema,
	}
}

func generateStreamQueryOptions(p parameter, index mockIndex) pbv1.StreamQueryOptions {
	timeRange := timestamp.TimeRange{
		Start:        time.Unix(int64(p.startTimestamp), 0),
		End:          time.Unix(int64(p.endTimestamp), 0),
		IncludeStart: true,
		IncludeEnd:   true,
	}
	entities := make([][]*modelv1.TagValue, 0)
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
		entities = append(entities, entity)
	}
	num := generateRandomNumber(int64(p.tagCardinality))
	value := filterTagValuePrefix + strconv.Itoa(num)
	filter := mockFilter{
		index: index,
		value: value,
	}
	indexRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{
			Id: uint32(1),
		},
		Tags: []string{"filter-tag"},
		Type: databasev1.IndexRule_TYPE_INVERTED,
	}
	order := &pbv1.OrderBy{
		Index: indexRule,
		Sort:  modelv1.Sort_SORT_ASC,
	}
	tagProjection := pbv1.TagProjection{
		Family: "benchmark-family",
		Names:  []string{"entity-tag", "filter-tag"},
	}
	return pbv1.StreamQueryOptions{
		Name:           "benchmark",
		TimeRange:      &timeRange,
		Entities:       entities,
		Filter:         filter,
		Order:          order,
		TagProjection:  []pbv1.TagProjection{tagProjection},
		MaxElementSize: math.MaxInt32,
	}
}

func BenchmarkFilter(b *testing.B) {
	b.ReportAllocs()
	for _, p := range pList {
		esList, docsList, idx := generateData(p)
		db := write(b, p, esList, docsList)
		s := generateStream(db)
		sqo := generateStreamQueryOptions(p, idx)
		sqo.Order = nil
		b.Run("filter-"+p.scenario, func(b *testing.B) {
			res, err := s.Query(context.TODO(), sqo)
			require.NoError(b, err)
			logicalstream.BuildElementsFromStreamResult(res)
		})
	}
}

func BenchmarkSort(b *testing.B) {
	b.ReportAllocs()
	for _, p := range pList {
		esList, docsList, idx := generateData(p)
		db := write(b, p, esList, docsList)
		s := generateStream(db)
		sqo := generateStreamQueryOptions(p, idx)
		b.Run("sort-"+p.scenario, func(b *testing.B) {
			res, err := s.Query(context.TODO(), sqo)
			require.NoError(b, err)
			logicalstream.BuildElementsFromStreamResult(res)
		})
	}
}
