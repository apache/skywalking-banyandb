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
	"errors"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

func validateQueryInput(sqo model.StreamQueryOptions) error {
	if sqo.TimeRange == nil || len(sqo.Entities) < 1 {
		return errors.New("invalid query options: timeRange and series are required")
	}
	if len(sqo.TagProjection) == 0 {
		return errors.New("invalid query options: tagProjection is required")
	}
	return nil
}

func (s *stream) getTSDB() (storage.TSDB[*tsTable, option], error) {
	var tsdb storage.TSDB[*tsTable, option]
	db := s.tsdb.Load()
	if db == nil {
		var err error
		tsdb, err = s.schemaRepo.loadTSDB(s.group)
		if err != nil {
			return nil, err
		}
		s.tsdb.Store(tsdb)
	} else {
		tsdb = db.(storage.TSDB[*tsTable, option])
	}
	return tsdb, nil
}

func prepareSeriesData(sqo model.StreamQueryOptions) []*pbv1.Series {
	series := make([]*pbv1.Series, len(sqo.Entities))
	for i := range sqo.Entities {
		series[i] = &pbv1.Series{
			Subject:      sqo.Name,
			EntityValues: sqo.Entities[i],
		}
	}
	return series
}

func prepareQueryOptions(sqo model.StreamQueryOptions, schemaTagTypes map[string]pbv1.ValueType) queryOptions {
	return queryOptions{
		StreamQueryOptions: sqo,
		schemaTagTypes:     schemaTagTypes,
		minTimestamp:       sqo.TimeRange.Start.UnixNano(),
		maxTimestamp:       sqo.TimeRange.End.UnixNano(),
	}
}

type queryOptions struct {
	elementFilter  posting.List
	seriesToEntity map[common.SeriesID][]*modelv1.TagValue
	sortedSids     []common.SeriesID
	schemaTagTypes map[string]pbv1.ValueType
	model.StreamQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func (qo *queryOptions) reset() {
	qo.StreamQueryOptions.Reset()
	qo.elementFilter = nil
	qo.seriesToEntity = nil
	qo.sortedSids = nil
	qo.schemaTagTypes = nil
	qo.minTimestamp = 0
	qo.maxTimestamp = 0
}

func (qo *queryOptions) copyFrom(other *queryOptions) {
	qo.StreamQueryOptions.CopyFrom(&other.StreamQueryOptions)
	qo.elementFilter = other.elementFilter
	qo.seriesToEntity = other.seriesToEntity
	qo.sortedSids = other.sortedSids
	qo.schemaTagTypes = other.schemaTagTypes
	qo.minTimestamp = other.minTimestamp
	qo.maxTimestamp = other.maxTimestamp
}

func mustEncodeTagValue(name string, tagType databasev1.TagType, tagValue *modelv1.TagValue, num int) [][]byte {
	values := make([][]byte, num)
	tv := encodeTagValue(name, tagType, tagValue)
	defer releaseTagValue(tv)
	value := tv.marshal()
	for i := 0; i < num; i++ {
		values[i] = value
	}
	return values
}

func mustDecodeTagValue(valueType pbv1.ValueType, value []byte) *modelv1.TagValue {
	if value == nil {
		return pbv1.NullTagValue
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		return int64TagValue(convert.BytesToInt64(value))
	case pbv1.ValueTypeStr:
		return strTagValue(string(value))
	case pbv1.ValueTypeBinaryData:
		return binaryDataTagValue(value)
	case pbv1.ValueTypeInt64Arr:
		var values []int64
		for i := 0; i < len(value); i += 8 {
			values = append(values, convert.BytesToInt64(value[i:i+8]))
		}
		return int64ArrTagValue(values)
	case pbv1.ValueTypeStrArr:
		var values []string
		bb := bigValuePool.Generate()
		defer bigValuePool.Release(bb)
		var err error
		for len(value) > 0 {
			bb.Buf, value, err = unmarshalVarArray(bb.Buf[:0], value)
			if err != nil {
				logger.Panicf("unmarshalVarArray failed: %v", err)
			}
			values = append(values, string(bb.Buf))
		}
		return strArrTagValue(values)
	default:
		logger.Panicf("unsupported value type: %v", valueType)
		return nil
	}
}

func int64TagValue(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Int{
			Int: &modelv1.Int{
				Value: value,
			},
		},
	}
}

func strTagValue(value string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_Str{
			Str: &modelv1.Str{
				Value: value,
			},
		},
	}
}

func binaryDataTagValue(value []byte) *modelv1.TagValue {
	data := make([]byte, len(value))
	copy(data, value)
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_BinaryData{
			BinaryData: data,
		},
	}
}

func int64ArrTagValue(values []int64) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_IntArray{
			IntArray: &modelv1.IntArray{
				Value: values,
			},
		},
	}
}

func strArrTagValue(values []string) *modelv1.TagValue {
	return &modelv1.TagValue{
		Value: &modelv1.TagValue_StrArray{
			StrArray: &modelv1.StrArray{
				Value: values,
			},
		},
	}
}

func updateTimeRange(filterTS posting.List, minTimestamp, maxTimestamp int64) (int64, int64) {
	if filterTS != nil && !filterTS.IsEmpty() {
		if minTS, err := filterTS.Min(); err == nil && int64(minTS) > minTimestamp {
			minTimestamp = int64(minTS)
		}
		if maxTS, err := filterTS.Max(); err == nil && int64(maxTS) < maxTimestamp {
			maxTimestamp = int64(maxTS)
		}
	}
	return minTimestamp, maxTimestamp
}

func loadBlockCursor(bc *blockCursor, tmpBlock *block, qo queryOptions, is indexSchema) bool {
	tmpBlock.reset()
	if !bc.loadData(tmpBlock) {
		releaseBlockCursor(bc)
		return false
	}
	entityValues := qo.seriesToEntity[bc.bm.seriesID]

	tagFamilyMap := make(map[string]int)
	for idx, tagFamily := range bc.tagFamilies {
		tagFamilyMap[tagFamily.name] = idx + 1
	}
	for _, tagFamilyProj := range bc.tagProjection {
		for j, tagProj := range tagFamilyProj.Names {
			tagSpec := is.tagMap[tagProj]
			if tagSpec == nil {
				continue
			}
			entityPos := is.indexRuleLocators.EntitySet[tagProj]
			if entityPos == 0 {
				continue
			}
			tagFamilyPos := tagFamilyMap[tagFamilyProj.Family]
			valueType := pbv1.MustTagValueToValueType(entityValues[entityPos-1])
			bc.tagFamilies[tagFamilyPos-1].tags[j] = tag{
				name:      tagProj,
				values:    mustEncodeTagValue(tagProj, tagSpec.GetType(), entityValues[entityPos-1], len(bc.timestamps)),
				valueType: valueType,
			}
		}
	}
	return true
}
