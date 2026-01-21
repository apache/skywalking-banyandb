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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	_               logical.UnresolvedPlan = (*unresolvedLocalScan)(nil)
	fieldProjection                        = []string{measure.TopNFieldName}
)

type unresolvedLocalScan struct {
	startTime   time.Time
	endTime     time.Time
	ec          executor.MeasureExecutionContext
	name        string
	conditions  []*modelv1.Condition
	groupByTags []string
	sort        modelv1.Sort
	number      int32
}

func (uls *unresolvedLocalScan) Analyze(s logical.Schema) (logical.Plan, error) {
	tr := timestamp.NewInclusiveTimeRange(uls.startTime, uls.endTime)
	groupByTags, err := uls.parseGroupByTags()
	if err != nil {
		return nil, errors.Wrap(err, "failed to locate entity")
	}
	entities := [][]*modelv1.TagValue{
		{
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: uls.name,
					},
				},
			},
			{
				Value: &modelv1.TagValue_Int{
					Int: &modelv1.Int{
						Value: int64(uls.sort),
					},
				},
			},
			pbv1.AnyTagValue,
			pbv1.AnyTagValue,
		},
	}
	if len(groupByTags) > 0 {
		entities[0][2] = &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: measure.GroupName(groupByTags),
				},
			},
		}
	}
	return &localScan{
		s: s,
		options: model.MeasureQueryOptions{
			Name:      measure.TopNSchemaName,
			TimeRange: &tr,
			Entities:  entities,
			TagProjection: []model.TagProjection{
				{
					Family: measure.TopNTagFamily,
					Names:  measure.TopNTagNames,
				},
			},
			FieldProjection: fieldProjection,
			Sort:            uls.sort,
			Number:          uls.number,
		},
		ec: uls.ec,
	}, nil
}

func (uls *unresolvedLocalScan) parseGroupByTags() ([]string, error) {
	if len(uls.conditions) == 0 {
		return nil, nil
	}
	entityMap := make(map[string]int)
	entity := make([]string, len(uls.groupByTags))
	for idx, tagName := range uls.groupByTags {
		entityMap[tagName] = idx
	}
	parsed := 0
	for _, pairQuery := range uls.conditions {
		if pairQuery.GetOp() != modelv1.Condition_BINARY_OP_EQ {
			return nil, errors.Errorf("tag belongs to the entity only supports EQ operation in condition(%v)", pairQuery)
		}
		if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
			switch pairQuery.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str, *modelv1.TagValue_Int, *modelv1.TagValue_Null:
				entity[entityIdx] = measure.Stringify(pairQuery.Value)
				parsed++
			default:
				return nil, errors.New("unsupported condition tag type for entity")
			}
			continue
		}
		return nil, errors.New("only groupBy tag name is supported")
	}
	if parsed != len(uls.groupByTags) {
		return nil, errors.New("failed to parse all groupBy tags")
	}

	return entity, nil
}

var _ logical.Plan = (*localScan)(nil)

type localScan struct {
	s       logical.Schema
	ec      executor.MeasureExecutionContext
	options model.MeasureQueryOptions
}

func (i *localScan) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	result, err := i.ec.Query(ctx, i.options)
	if err != nil {
		return nil, fmt.Errorf("failed to query measure: %w", err)
	}
	return &topNMIterator{
		result: result,
	}, nil
}

func (i *localScan) String() string {
	return fmt.Sprintf("TopNAggScan: %s startTime=%d,endTime=%d, entity=%s;",
		i.options.Name, i.options.TimeRange.Start.Unix(), i.options.TimeRange.End.Unix(),
		i.options.Entities)
}

func (i *localScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localScan) Schema() logical.Schema {
	return i.s
}

type topNMIterator struct {
	result  model.MeasureQueryResult
	err     error
	current []*measurev1.InternalDataPoint
}

func (ei *topNMIterator) Next() bool {
	if ei.result == nil {
		return false
	}

	r := ei.result.Pull()
	if r == nil {
		return false
	}
	if r.Error != nil {
		ei.err = r.Error
		return false
	}
	ei.current = ei.current[:0]
	topNValue := measure.GenerateTopNValue()
	defer measure.ReleaseTopNValue(topNValue)
	decoder := measure.GenerateTopNValuesDecoder()
	defer measure.ReleaseTopNValuesDecoder(decoder)

	for i := range r.Timestamps {
		fv := r.Fields[0].Values[i]
		bd := fv.GetBinaryData()
		if bd == nil {
			ei.err = errors.New("failed to get binary data")
			return false
		}
		ts := timestamppb.New(time.Unix(0, r.Timestamps[i]))
		topNValue.Reset()
		err := topNValue.Unmarshal(bd, decoder)
		if err != nil {
			ei.err = multierr.Append(ei.err, errors.WithMessagef(err, "failed to unmarshal topN values[%d]:[%s]%s", i, ts, hex.EncodeToString(fv.GetBinaryData())))
			continue
		}
		shardID := uint32(0)
		if i < len(r.ShardIDs) {
			shardID = uint32(r.ShardIDs[i])
		}
		fieldName, entityNames, values, entities := topNValue.Values()
		for j := range entities {
			dp := &measurev1.DataPoint{
				Timestamp: ts,
				Sid:       uint64(r.SID),
				Version:   r.Versions[i],
			}
			tagFamily := &modelv1.TagFamily{
				Name: measure.TopNTagFamily,
			}
			dp.TagFamilies = append(dp.TagFamilies, tagFamily)
			for k, entityName := range entityNames {
				tagFamily.Tags = append(tagFamily.Tags, &modelv1.Tag{
					Key:   entityName,
					Value: entities[j][k],
				})
			}
			dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
				Name: fieldName,
				Value: &modelv1.FieldValue{
					Value: &modelv1.FieldValue_Int{
						Int: &modelv1.Int{
							Value: values[j],
						},
					},
				},
			})
			ei.current = append(ei.current, &measurev1.InternalDataPoint{DataPoint: dp, ShardId: shardID})
		}
	}
	return true
}

func (ei *topNMIterator) Current() []*measurev1.InternalDataPoint {
	return ei.current
}

func (ei *topNMIterator) Close() error {
	if ei.result != nil {
		ei.result.Release()
		ei.result = nil
	}
	return ei.err
}
