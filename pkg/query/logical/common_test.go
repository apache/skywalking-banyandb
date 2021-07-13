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
	"github.com/apache/skywalking-banyandb/pkg/fb"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	executor2 "github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical2 "github.com/apache/skywalking-banyandb/pkg/query/logical"
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
	chunkIDs []common.ChunkID
	ptr      int
}

func (a *arrayChunkIDGenerator) Next() common.ChunkID {
	defer func() {
		a.ptr++
	}()
	return a.chunkIDs[a.ptr]
}

func (a *arrayChunkIDGenerator) HasNext() bool {
	return a.ptr < len(a.chunkIDs)
}

func GeneratorFromArray(chunkIDs []common.ChunkID) ChunkIDGenerator {
	return &arrayChunkIDGenerator{
		chunkIDs: chunkIDs,
		ptr:      0,
	}
}

func GenerateEntities(g ChunkIDGenerator) []data.Entity {
	entities := make([]data.Entity, 0)
	rand.Seed(time.Now().UnixNano())
	for g.HasNext() {
		b := fb.NewQueryEntityBuilder()
		et := b.BuildEntity(
			b.BuildEntityID(strconv.FormatUint(uint64(g.Next()), 10)),
			b.BuildFields("trace_id", generateRndServiceName(rand.Int63()), "http.method", "GET"),
			b.BuildTimeStamp(time.Now()),
		)
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
	s             logical2.Schema
}

func NewMockDataFactory(ctrl *gomock.Controller, traceMetadata *common.Metadata, s logical2.Schema, num int) *mockDataFactory {
	return &mockDataFactory{
		ctrl:          ctrl,
		num:           num,
		traceMetadata: traceMetadata,
		s:             s,
	}
}

func (f *mockDataFactory) MockParentPlan() logical2.UnresolvedPlan {
	p := logical.NewMockPlan(f.ctrl)
	p.EXPECT().Execute(gomock.Any()).Return(GenerateEntities(GeneratorFromRange(0, common.ChunkID(f.num-1))), nil)
	p.EXPECT().Schema().Return(f.s).AnyTimes()
	up := logical.NewMockUnresolvedPlan(f.ctrl)
	up.EXPECT().Analyze(f.s).Return(p, nil)
	return up
}

func (f *mockDataFactory) MockTraceIDFetch(traceID string) executor2.ExecutionContext {
	ec := executor.NewMockExecutionContext(f.ctrl)
	ec.EXPECT().FetchTrace(*f.traceMetadata, traceID, series.ScanOptions{}).Return(data.Trace{
		KindVersion: common.KindVersion{},
		Entities:    GenerateEntities(GeneratorFromRange(0, common.ChunkID(f.num-1))),
	}, nil)
	return ec
}

func (f *mockDataFactory) MockIndexScan(startTime, endTime time.Time, indexMatches ...*indexMatcher) executor2.ExecutionContext {
	ec := executor.NewMockExecutionContext(f.ctrl)
	for _, im := range indexMatches {
		ec.
			EXPECT().
			Search(*f.traceMetadata, uint64(startTime.UnixNano()), uint64(endTime.UnixNano()), im).
			Return(im.chunkIDs, nil)
	}
	ec.
		EXPECT().
		FetchEntity(*f.traceMetadata, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ common.Metadata, chunkIDs []common.ChunkID, _ series.ScanOptions) ([]data.Entity, error) {
			return GenerateEntities(GeneratorFromArray(chunkIDs)), nil
		})
	return ec
}

func prepareSchema(assert *require.Assertions) (*common.Metadata, logical2.Schema) {
	ana := logical2.DefaultAnalyzer()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	builder := fb.NewCriteriaBuilder()
	criteria := builder.BuildEntityCriteria(
		fb.AddLimit(0),
		fb.AddOffset(0),
		builder.BuildMetaData("default", "trace"),
		builder.BuildTimeStampNanoSeconds(sT, eT),
	)

	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        criteria.Metadata(nil),
	}

	schema, err := ana.BuildTraceSchema(context.TODO(), *metadata)
	assert.NoError(err)
	return metadata, schema
}

var _ gomock.Matcher = (*indexMatcher)(nil)

type indexMatcher struct {
	key      string
	chunkIDs []common.ChunkID
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

func NewIndexMatcher(key string, chunkIDs []common.ChunkID) *indexMatcher {
	return &indexMatcher{
		key:      key,
		chunkIDs: chunkIDs,
	}
}
