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

// // Package stream implements a time-series-based storage which is consists of a sequence of element.
// // Each element drops in a arbitrary interval. They are immutable, can not be updated or overwritten.
// package stream

// import (
// 	"context"

// 	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
// 	"github.com/apache/skywalking-banyandb/banyand/tsdb"
// 	"github.com/apache/skywalking-banyandb/banyand/tsdb/index"
// 	"github.com/apache/skywalking-banyandb/pkg/logger"
// )

// // a chunk is 1MB.
// const chunkSize = 1 << 20

// type stream struct {
// 	db          tsdb.Supplier
// 	l           *logger.Logger
// 	schema      *databasev1.Stream
// 	indexWriter *index.Writer
// 	name        string
// 	group       string
// 	indexRules  []*databasev1.IndexRule
// 	shardNum    uint32
// }

// func (s *stream) GetSchema() *databasev1.Stream {
// 	return s.schema
// }

// func (s *stream) GetIndexRules() []*databasev1.IndexRule {
// 	return s.indexRules
// }

// func (s *stream) Close() error {
// 	return nil
// }

// func (s *stream) parseSpec() {
// 	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
// }

// type streamSpec struct {
// 	schema     *databasev1.Stream
// 	indexRules []*databasev1.IndexRule
// }

// func openStream(shardNum uint32, db tsdb.Supplier, spec streamSpec, l *logger.Logger) *stream {
// 	sm := &stream{
// 		shardNum:   shardNum,
// 		schema:     spec.schema,
// 		indexRules: spec.indexRules,
// 		l:          l,
// 	}
// 	sm.parseSpec()
// 	ctx := context.WithValue(context.Background(), logger.ContextKey, l)

// 	if db == nil {
// 		return sm
// 	}
// 	sm.db = db
// 	sm.indexWriter = index.NewWriter(ctx, index.WriterOptions{
// 		DB:                db,
// 		ShardNum:          shardNum,
// 		Families:          spec.schema.TagFamilies,
// 		IndexRules:        spec.indexRules,
// 		EnableGlobalIndex: true,
// 	})
// 	return sm
// }

// // Package stream implements a time-series-based storage which is consists of a sequence of element.
// // Each element drops in a arbitrary interval. They are immutable, can not be updated or overwritten.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"

	// "github.com/apache/skywalking-banyandb/pkg/index/posting"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/schema"
)

const (
	maxValuesBlockSize              = 8 * 1024 * 1024
	maxTimestampsBlockSize          = 8 * 1024 * 1024
	maxElementIDsBlockSize          = 8 * 1024 * 1024
	maxTagFamiliesMetadataSize      = 8 * 1024 * 1024
	maxUncompressedBlockSize        = 2 * 1024 * 1024
	maxUncompressedPrimaryBlockSize = 128 * 1024

	maxBlockLength = 8 * 1024
)

// var errTagFamilyNotExist = errors.New("tag family doesn't exist")

// Query allow to retrieve elements in a series of streams.
type Query interface {
	Stream(stream *commonv1.Metadata) (Stream, error)
}

// Stream allows inspecting elements' details.
type Stream interface {
	io.Closer
	// Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
	// Shard(id common.ShardID) (tsdb.Shard, error)
	// ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error)
	ParseElementIDDeprecated(item tsdb.Item) (string, error)
	GetSchema() *databasev1.Stream
	GetIndexRules() []*databasev1.IndexRule
	Query(ctx context.Context, opts pbv1.StreamQueryOptions) (pbv1.StreamQueryResult, error)
}

var _ Stream = (*stream)(nil)

type stream struct {
	// db                tsdb.Supplier
	l      *logger.Logger
	schema *databasev1.Stream
	// indexWriter       *index.Writer
	name              string
	group             string
	indexRules        []*databasev1.IndexRule
	shardNum          uint32
	indexRuleLocators []*partition.IndexRuleLocator
	databaseSupplier  schema.Supplier
}

// type stream struct {
// 	databaseSupplier  schema.Supplier
// 	l                 *logger.Logger
// 	schema            *databasev1.Stream
// 	name              string
// 	group             string
// 	indexRules        []*databasev1.IndexRule
// 	indexRuleLocators []*partition.IndexRuleLocator
// 	shardNum          uint32
// }

func (s *stream) GetSchema() *databasev1.Stream {
	return s.schema
}

func (s *stream) GetIndexRules() []*databasev1.IndexRule {
	return s.indexRules
}

func (s *stream) Close() error {
	return nil
}

func (s *stream) parseSpec() (err error) {
	s.name, s.group = s.schema.GetMetadata().GetName(), s.schema.GetMetadata().GetGroup()
	s.indexRuleLocators = partition.ParseIndexRuleLocators(s.schema.GetTagFamilies(), s.indexRules)

	return err
}

func (s *stream) ParseElementIDDeprecated(item tsdb.Item) (string, error) {
	rawBytes, err := item.Val()
	if err != nil {
		return "", err
	}
	return string(rawBytes), nil
}

func (s *stream) Query(ctx context.Context, sqo pbv1.StreamQueryOptions) (pbv1.StreamQueryResult, error) {
	if sqo.TimeRange == nil || sqo.Entity == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(sqo.TagProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection or fieldProjection is required")
	}
	tsdb := s.databaseSupplier.SupplyTSDB().(storage.TSDB[*tsTable])
	tabWrappers := tsdb.SelectTSTables(*sqo.TimeRange)
	sl, err := tsdb.Lookup(ctx, &pbv1.Series{Subject: sqo.Name, EntityValues: sqo.Entity})
	if err != nil {
		return nil, err
	}

	var result queryResult
	if len(sl) < 1 {
		return &result, nil
	}
	var sids []common.SeriesID
	for i := range sl {
		sids = append(sids, sl[i].ID)
	}
	var pws []*partWrapper
	var parts []*part
	qo := queryOptions{
		StreamQueryOptions: sqo,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
	}
	for _, tw := range tabWrappers {
		pws, parts = tw.Table().getParts(pws, parts, qo)
	}
	// TODO: cache tstIter
	var tstIter tstIter
	originalSids := make([]common.SeriesID, len(sids))
	copy(originalSids, sids)
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	tstIter.init(parts, sids, qo.minTimestamp, qo.maxTimestamp)
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot init tstIter: %w", tstIter.Error())
	}
	for tstIter.nextBlock() {
		bc := generateBlockCursor()
		p := tstIter.piHeap[0]
		bc.init(p.p, p.curBlock, qo)
		result.data = append(result.data, bc)
	}
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot iterate tstIter: %w", tstIter.Error())
	}
	if sqo.Order == nil {
		result.orderByTS = true
		result.ascTS = true
		return &result, nil
	}
	if sqo.Order.Index == nil {
		result.orderByTS = true
		if sqo.Order.Sort == modelv1.Sort_SORT_ASC || sqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.ascTS = true
		}
		return &result, nil
	}

	result.sidToIndex = make(map[common.SeriesID]int)
	for i, si := range originalSids {
		result.sidToIndex[si] = i
	}
	return &result, nil
}

type streamSpec struct {
	schema     *databasev1.Stream
	indexRules []*databasev1.IndexRule
}

func openStream(shardNum uint32, db schema.Supplier, spec streamSpec, l *logger.Logger, pipeline queue.Queue,
) (*stream, error) {
	s := &stream{
		shardNum:   shardNum,
		schema:     spec.schema,
		indexRules: spec.indexRules,
		l:          l,
	}
	if err := s.parseSpec(); err != nil {
		return nil, err
	}
	if db == nil {
		return s, nil
	}

	s.databaseSupplier = db
	return s, nil
}
