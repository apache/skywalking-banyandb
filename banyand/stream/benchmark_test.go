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
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	segmentMetadataFilename = "metadata"
	version                 = "1.0.0"
	entityTagValuePrefix    = "entity"
	filterTagValuePrefix    = "value"
	testGroupName           = "test"
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

func generateRandomNumber(maxValue int64) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(maxValue))
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
			elementIDs:  []uint64{},
			tagFamilies: [][]tagValues{},
		}
		var docs index.Documents
		for j := 1; j <= p.timestampCount; j++ {
			timestamp := i*p.timestampCount + j
			unixTimestamp := time.Unix(int64(timestamp), 0).UnixNano()
			for k := 1; k <= p.seriesCount; k++ {
				elementID := strconv.Itoa(k) + strconv.Itoa(timestamp)
				es.seriesIDs = append(es.seriesIDs, common.SeriesID(k))
				es.elementIDs = append(es.elementIDs, convert.HashStr(elementID))
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

				fields = append(fields, index.NewBytesField(index.FieldKey{
					IndexRuleID: 1,
					SeriesID:    common.SeriesID(k),
				}, []byte(value)))
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

func openDatabase(b testing.TB, path string) storage.TSDB[*tsTable, option] {
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
			mergePolicy: newDefaultMergePolicyForTesting(),
			protector:   protector.Nop{},
		},
	}

	group := testGroupName
	db, err := storage.OpenTSDB(
		common.SetPosition(context.Background(), func(p common.Position) common.Position {
			p.Module = "stream"
			p.Database = "benchmark"
			return p
		}),
		opts, nil, group,
	)
	require.NoError(b, err)
	return db
}
