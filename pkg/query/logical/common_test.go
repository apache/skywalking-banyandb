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

package logical_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/pb"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

type ChunkIDGenerator interface {
	Next() common.ChunkID
	HasNext() bool
}

var _ ChunkIDGenerator = (*rangeChunkIDGenerator)(nil)

type rangeChunkIDGenerator struct {
	left  common.ChunkID
	right common.ChunkID
	ptr   common.ChunkID
}

func (r *rangeChunkIDGenerator) Next() common.ChunkID {
	defer func() {
		r.ptr++
	}()
	return r.ptr
}

func (r *rangeChunkIDGenerator) HasNext() bool {
	return r.ptr <= r.right
}

func GeneratorFromRange(l, r common.ChunkID) ChunkIDGenerator {
	return &rangeChunkIDGenerator{
		left:  l,
		right: r,
		ptr:   l,
	}
}

var _ ChunkIDGenerator = (*arrayChunkIDGenerator)(nil)

type arrayChunkIDGenerator struct {
	hasNext bool
	iter    posting.Iterator
}

func (a *arrayChunkIDGenerator) Next() common.ChunkID {
	a.hasNext = a.iter.Next()
	return a.iter.Current()
}

func (a *arrayChunkIDGenerator) HasNext() bool {
	return a.hasNext
}

func GeneratorFromArray(chunkIDs posting.List) ChunkIDGenerator {
	iter := chunkIDs.Iterator()
	return &arrayChunkIDGenerator{
		iter:    iter,
		hasNext: iter.Next(),
	}
}

func GenerateEntities(g ChunkIDGenerator) []data.Entity {
	entities := make([]data.Entity, 0)
	rand.Seed(time.Now().UnixNano())
	for g.HasNext() {
		et := pb.NewQueryEntityBuilder().
			EntityID(strconv.FormatUint(uint64(g.Next()), 10)).
			Timestamp(time.Now()).
			Fields("trace_id", generateRndServiceName(rand.Int63()), "http.method", "GET").
			Build()

		entities = append(entities, data.Entity{Entity: et})
	}
	return entities
}

func generateRndServiceName(rndNum int64) string {
	h := sha256.Sum256([]byte(strconv.FormatInt(rndNum, 10)))
	return base64.StdEncoding.EncodeToString(h[:])
}

type mockDataFactory struct {
	ctrl          *gomock.Controller
	num           int
	traceMetadata *common.Metadata
	s             logical.Schema
}

func newMockDataFactory(ctrl *gomock.Controller, traceMetadata *common.Metadata, s logical.Schema, num int) *mockDataFactory {
	return &mockDataFactory{
		ctrl:          ctrl,
		num:           num,
		traceMetadata: traceMetadata,
		s:             s,
	}
}

func (f *mockDataFactory) MockParentPlan() logical.UnresolvedPlan {
	p := logical.NewMockPlan(f.ctrl)
	p.EXPECT().Execute(gomock.Any()).Return(GenerateEntities(GeneratorFromRange(0, common.ChunkID(f.num-1))), nil)
	p.EXPECT().Schema().Return(f.s).AnyTimes()
	up := logical.NewMockUnresolvedPlan(f.ctrl)
	up.EXPECT().Analyze(f.s).Return(p, nil)
	return up
}

func (f *mockDataFactory) MockTraceIDFetch(traceID string) executor.ExecutionContext {
	ec := executor.NewMockExecutionContext(f.ctrl)
	ec.EXPECT().FetchTrace(*f.traceMetadata, traceID, series.ScanOptions{}).Return(data.Trace{
		KindVersion: common.KindVersion{},
		Entities:    GenerateEntities(GeneratorFromRange(0, common.ChunkID(f.num-1))),
	}, nil)
	return ec
}

func (f *mockDataFactory) MockIndexScan(startTime, endTime time.Time, indexMatches ...*indexMatcher) executor.ExecutionContext {
	ec := executor.NewMockExecutionContext(f.ctrl)
	usedShards := make(map[uint]posting.List)

	for _, im := range indexMatches {
		ec.
			EXPECT().
			Search(*f.traceMetadata, gomock.Eq(im.shardID), uint64(startTime.UnixNano()), uint64(endTime.UnixNano()), gomock.Any(), im).
			Return(im.chunkIDs, nil)

		if list, ok := usedShards[im.shardID]; ok {
			_ = list.Intersect(im.chunkIDs)
		} else {
			usedShards[im.shardID] = im.chunkIDs
		}

		ec.
			EXPECT().
			Search(*f.traceMetadata, gomock.Not(gomock.Eq(im.shardID)), uint64(startTime.UnixNano()), uint64(endTime.UnixNano()), gomock.Any(), im).
			Return(roaring.NewPostingList(), nil)
	}

	for shardID := uint(0); shardID < uint(f.s.ShardNumber()); shardID++ {
		if chunkList, ok := usedShards[shardID]; ok && chunkList.Len() > 0 {
			ec.
				EXPECT().
				FetchEntity(*f.traceMetadata, shardID, gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ common.Metadata, _ uint, chunkIDs posting.List, _ series.ScanOptions) ([]data.Entity, error) {
					return GenerateEntities(GeneratorFromArray(chunkIDs)), nil
				})
		}
	}

	return ec
}

func prepareSchema(assert *require.Assertions) (*common.Metadata, logical.Schema) {
	ana := logical.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := pb.NewQueryRequestBuilder().
		Limit(0).Offset(0).
		Metadata("default", "trace").
		TimeRange(sT, eT).
		Build()

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.GetMetadata(),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)
	return metadata, schema
}

var _ gomock.Matcher = (*indexMatcher)(nil)

type indexMatcher struct {
	key      string
	shardID  uint
	chunkIDs posting.List
}

func (i *indexMatcher) Matches(x interface{}) bool {
	if conds, ok := x.([]index.Condition); ok {
		for _, cond := range conds {
			if cond.Key == i.key {
				return true
			}
		}
	}
	return false
}

func (i *indexMatcher) String() string {
	return fmt.Sprintf("is search for key %s", i.key)
}

func newIndexMatcher(key string, shardID uint, chunkIDs posting.List) *indexMatcher {
	return &indexMatcher{
		key:      key,
		shardID:  shardID,
		chunkIDs: chunkIDs,
	}
}
