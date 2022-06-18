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

package query

import (
	"bytes"
	"container/heap"
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type topNQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (t *topNQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	request, ok := message.Data().(*measurev1.TopNRequest)
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	t.log.Info().Msg("received a topN query event")
	topNMetadata := request.GetMetadata()
	topNSchema, err := t.metaService.TopNAggregationRegistry().GetTopNAggregation(context.TODO(), topNMetadata)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to get execution context")
		return
	}
	sourceMeasure, err := t.measureService.Measure(topNSchema.GetSourceMeasure())
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to find source measure")
		return
	}
	shards, err := sourceMeasure.CompanionShards(topNMetadata)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to list shards")
		return
	}
	aggregator := createTopNPostAggregator(request.GetTopN(),
		request.GetAgg(), request.GetFieldValueSort())
	entity, err := locateEntity(topNSchema, request.GetConditions())
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to parse entity")
		return
	}
	for _, shard := range shards {
		// TODO: support condition
		sl, innerErr := shard.Series().List(tsdb.NewPath(entity))
		if innerErr != nil {
			t.log.Error().Err(innerErr).
				Str("topN", topNMetadata.GetName()).
				Msg("fail to list series")
			return
		}
		for _, series := range sl {
			iters, scanErr := t.scanSeries(series, request)
			if scanErr != nil {
				t.log.Error().Err(innerErr).
					Str("topN", topNMetadata.GetName()).
					Msg("fail to scan series")
				return
			}
			for _, iter := range iters {
				defer func(iter tsdb.ItemIterator) {
					_ = iter.Close()
				}(iter)
				for {
					if item, hasNext := iter.Next(); hasNext {
						tuple, parseErr := parseTopNFamily(item, sourceMeasure.GetInterval())
						if parseErr != nil {
							t.log.Error().Err(parseErr).
								Str("topN", topNMetadata.GetName()).
								Msg("fail to parse topN family")
							return
						}
						_ = aggregator.put(tuple.V1.(string), tuple.V2.(int64))
					} else {
						break
					}
				}
			}
		}
	}

	itemLen := int(math.Min(float64(request.GetTopN()), float64(aggregator.Len())))
	topNItems := make([]*measurev1.TopNList_Item, 0, itemLen)

	for _, item := range aggregator.items[0:itemLen] {
		topNItems = append(topNItems, &measurev1.TopNList_Item{
			Name: item.key,
			Value: &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{Value: item.int64Func.Val()},
				},
			},
		})
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), topNItems)

	return
}

func locateEntity(topNSchema *databasev1.TopNAggregation, conditions []*modelv1.Condition) (tsdb.Entity, error) {
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, 1+len(topNSchema.GetGroupByTagNames()))
	entity[0] = tsdb.AnyEntry
	for idx, tagName := range topNSchema.GetGroupByTagNames() {
		entityMap[tagName] = idx + 1
		// fill AnyEntry by default
		entity[idx+1] = tsdb.AnyEntry
	}
	for _, pairQuery := range conditions {
		// TODO: check op?
		if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
			switch v := pairQuery.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str:
				entity[entityIdx] = []byte(v.Str.GetValue())
			case *modelv1.TagValue_Id:
				entity[entityIdx] = []byte(v.Id.GetValue())
			case *modelv1.TagValue_Int:
				entity[entityIdx] = convert.Int64ToBytes(v.Int.GetValue())
			default:
				return nil, errors.New("unsupported condition tag type for entity")
			}
			continue
		}
		return nil, errors.New("only groupBy tag name is supported")
	}
	return entity, nil
}

func parseTopNFamily(item tsdb.Item, interval time.Duration) (*streaming.Tuple2, error) {
	familyRawBytes, err := item.Family(familyIdentity(measure.TopNTagFamily, measure.TagFlag))
	if err != nil {
		return nil, err
	}
	tagFamily := &modelv1.TagFamilyForWrite{}
	err = proto.Unmarshal(familyRawBytes, tagFamily)
	if err != nil {
		return nil, err
	}
	fieldBytes, err := item.Family(familyIdentity(measure.TopNValueFieldSpec.GetName(),
		measure.EncoderFieldFlag(measure.TopNValueFieldSpec, interval)))
	if err != nil {
		return nil, err
	}
	fieldValue := measure.DecodeFieldValue(fieldBytes, measure.TopNValueFieldSpec)
	return &streaming.Tuple2{
		// GroupValues
		V1: tagFamily.GetTags()[1].GetStr().GetValue(),
		// FieldValue
		V2: fieldValue.GetInt().GetValue(),
	}, nil
}

func familyIdentity(name string, flag []byte) []byte {
	return bytes.Join([][]byte{tsdb.Hash([]byte(name)), flag}, nil)
}

func (t *topNQueryProcessor) scanSeries(series tsdb.Series, request *measurev1.TopNRequest) ([]tsdb.ItemIterator, error) {
	seriesSpan, err := series.Span(timestamp.NewInclusiveTimeRange(
		request.GetTimeRange().GetBegin().AsTime(),
		request.GetTimeRange().GetEnd().AsTime()),
	)
	defer func(seriesSpan tsdb.SeriesSpan) {
		_ = seriesSpan.Close()
	}(seriesSpan)
	if err != nil {
		return nil, err
	}
	seeker, err := seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_ASC).Build()
	if err != nil {
		return nil, err
	}
	return seeker.Seek()
}

var _ heap.Interface = (*topNPostAggregator)(nil)

type topNAggregatorItem struct {
	key       string
	int64Func aggregation.Int64Func
	index     int
}

type topNPostAggregator struct {
	topN     int32
	sort     modelv1.Sort
	aggrFunc modelv1.AggregationFunction
	items    []*topNAggregatorItem
	cache    map[string]*topNAggregatorItem
}

func (aggr topNPostAggregator) Len() int {
	return len(aggr.items)
}

func (aggr topNPostAggregator) Less(i, j int) bool {
	if aggr.sort == modelv1.Sort_SORT_DESC {
		return aggr.items[i].int64Func.Val() > aggr.items[j].int64Func.Val()
	}
	return aggr.items[i].int64Func.Val() < aggr.items[j].int64Func.Val()
}

func (aggr *topNPostAggregator) Swap(i, j int) {
	aggr.items[i], aggr.items[j] = aggr.items[j], aggr.items[i]
	aggr.items[i].index = i
	aggr.items[j].index = j
}

func (aggr *topNPostAggregator) Push(x any) {
	n := len(aggr.items)
	item := x.(*topNAggregatorItem)
	item.index = n
	aggr.items = append(aggr.items, item)
}

func (aggr *topNPostAggregator) Pop() any {
	old := aggr.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	aggr.items = old[0 : n-1]
	return item
}

func createTopNPostAggregator(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) *topNPostAggregator {
	aggregator := &topNPostAggregator{
		topN:     topN,
		sort:     sort,
		aggrFunc: aggrFunc,
		cache:    make(map[string]*topNAggregatorItem),
	}
	heap.Init(aggregator)
	return aggregator
}

func (aggr *topNPostAggregator) put(key string, val int64) error {
	if item, found := aggr.cache[key]; found {
		item.int64Func.In(val)
		heap.Fix(aggr, item.index)
		return nil
	}
	aggrFunc, err := aggregation.NewInt64Func(aggr.aggrFunc)
	if err != nil {
		return err
	}
	item := &topNAggregatorItem{
		key:       key,
		int64Func: aggrFunc,
	}
	item.int64Func.In(val)
	aggr.cache[key] = item
	heap.Push(aggr, item)
	return nil
}
