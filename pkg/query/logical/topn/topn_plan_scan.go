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

package topn

import (
	"bytes"
	"context"
	"fmt"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
	"io"
	"time"
)

type unresolvedScan struct {
	startTime     time.Time
	endTime       time.Time
	metadata      *commonv1.Metadata
	conditions    []*modelv1.Condition
	sortDirection modelv1.Sort
}

func newUnresolvedScan(criteria *measurev1.TopNRequest) logical.UnresolvedPlan {
	timeRange := criteria.GetTimeRange()
	return &unresolvedScan{
		startTime:     timeRange.GetBegin().AsTime(),
		endTime:       timeRange.GetEnd().AsTime(),
		metadata:      criteria.GetMetadata(),
		conditions:    criteria.GetConditions(),
		sortDirection: criteria.GetFieldValueSort(),
	}
}

func (us *unresolvedScan) Analyze(s logical.Schema) (logical.Plan, error) {
	tagList := s.EntityList()
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, 1+1+len(tagList))
	// sortDirection
	entity[0] = convert.Int64ToBytes(int64(us.sortDirection.Number()))
	// rankNumber
	entity[1] = tsdb.AnyEntry
	for idx, tagName := range tagList {
		entityMap[tagName] = idx + 2
		// allow to make fuzzy search with partial conditions
		entity[idx+2] = tsdb.AnyEntry
	}

	for _, pairQuery := range us.conditions {
		criteria := &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: pairQuery,
			},
		}
		_, es, err := logical.BuildLocalFilter(criteria, s, entityMap, entity)
		if err != nil {
			return nil, err
		}

		entity = es[0]
	}

	return &localScan{
		timeRange: timestamp.NewInclusiveTimeRange(us.startTime, us.endTime),
		schema:    s,
		metadata:  us.metadata,
		entity:    entity,
		l:         logger.GetLogger("topn", "measure", us.metadata.Group, us.metadata.Name, "local-scan"),
	}, nil
}

type localScan struct {
	schema    logical.Schema
	filter    index.Filter
	metadata  *commonv1.Metadata
	l         *logger.Logger
	timeRange timestamp.TimeRange
	entity    tsdb.Entity
}

func (ls *localScan) Execute(ec executor.TopNExecutionContext) (mit executor.TIterator, err error) {
	var seriesList tsdb.SeriesList
	shards, err := ec.CompanionShards(ls.metadata)
	if err != nil {
		ls.l.Error().Err(err).
			Str("topN", ls.metadata.GetName()).
			Msg("fail to list shards")
		return nil, err
	}
	for _, shard := range shards {
		sl, innerErr := shard.Series().List(context.WithValue(
			context.Background(),
			logger.ContextKey,
			ls.l,
		), tsdb.NewPath(ls.entity))
		if innerErr != nil {
			ls.l.Error().Err(innerErr).
				Str("topN", ls.metadata.GetName()).
				Msg("fail to list series")
			return nil, innerErr
		}
		seriesList = seriesList.Merge(sl)
	}
	if len(seriesList) == 0 {
		return dummyIter, nil
	}
	var builders []logical.SeekerBuilder
	builders = append(builders, func(builder tsdb.SeekerBuilder) {
		builder.OrderByTime(modelv1.Sort_SORT_ASC)
	})
	iters, closers, err := logical.ExecuteForShard(ls.l, seriesList, ls.timeRange, builders...)
	if err != nil {
		return nil, err
	}
	if len(closers) > 0 {
		defer func(closers []io.Closer) {
			for _, c := range closers {
				err = multierr.Append(err, c.Close())
			}
		}(closers)
	}

	if len(iters) == 0 {
		return dummyIter, nil
	}

	return newSeriesMIterator(iters, ec, 100), nil
}

func (ls *localScan) String() string {
	return fmt.Sprintf("LocalScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s;",
		ls.timeRange.Start.Unix(), ls.timeRange.End.Unix(), ls.metadata.GetGroup(), ls.metadata.GetName())
}

func (ls *localScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (ls *localScan) Schema() logical.Schema {
	return ls.schema
}

type seriesIterator struct {
	err      error
	interval time.Duration
	inner    []tsdb.Iterator
	current  []*streaming.Tuple2
	index    int
	num      int
	max      int
}

func (si *seriesIterator) Next() bool {
	if si.err != nil || si.num > si.max {
		return false
	}
	si.index++
	if si.index >= len(si.inner) {
		return false
	}
	iter := si.inner[si.index]
	if si.current != nil {
		si.current = si.current[:0]
	}
	for iter.Next() {
		tuple, err := transform(iter.Val(), si.interval)
		if err != nil {
			si.err = err
			return false
		}
		si.current = append(si.current, tuple)
	}
	si.num++
	return true
}

func (si *seriesIterator) Current() []*streaming.Tuple2 {
	return si.current
}

func (si *seriesIterator) Close() error {
	for _, i := range si.inner {
		si.err = multierr.Append(si.err, i.Close())
	}
	return si.err
}

func newSeriesMIterator(inner []tsdb.Iterator, ec executor.TopNExecutionContext, max int) executor.TIterator {
	return &seriesIterator{
		inner:    inner,
		interval: ec.(measure.Measure).GetInterval(),
		max:      max,
		index:    -1,
	}
}

func transform(item tsdb.Item, interval time.Duration) (*streaming.Tuple2, error) {
	familyRawBytes, err := item.Family(familyIdentity(measure.TopNTagFamily, pbv1.TagFlag))
	if err != nil {
		return nil, err
	}
	tagFamily := &modelv1.TagFamilyForWrite{}
	err = proto.Unmarshal(familyRawBytes, tagFamily)
	if err != nil {
		return nil, err
	}
	fieldBytes, err := item.Family(familyIdentity(measure.TopNValueFieldSpec.GetName(),
		pbv1.EncoderFieldFlag(measure.TopNValueFieldSpec, interval)))
	if err != nil {
		return nil, err
	}
	fieldValue, err := pbv1.DecodeFieldValue(fieldBytes, measure.TopNValueFieldSpec)
	return &streaming.Tuple2{
		// GroupValues
		V1: tagFamily.GetTags()[1:],
		// FieldValue
		V2: fieldValue,
	}, nil
}

func familyIdentity(name string, flag []byte) []byte {
	return bytes.Join([][]byte{tsdb.Hash([]byte(name)), flag}, nil)
}

var dummyIter = dummyIterator{}

type dummyIterator struct{}

func (ei dummyIterator) Next() bool {
	return false
}

func (ei dummyIterator) Current() []*streaming.Tuple2 {
	return nil
}

func (ei dummyIterator) Close() error {
	return nil
}
