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

package trace

import (
	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type queryOptions struct {
	elementFilter  posting.List
	seriesToEntity map[common.SeriesID][]*modelv1.TagValue
	sortedSids     []common.SeriesID
	model.StreamQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func (qo *queryOptions) reset() {
	qo.StreamQueryOptions.Reset()
	qo.elementFilter = nil
	qo.seriesToEntity = nil
	qo.sortedSids = nil
	qo.minTimestamp = 0
	qo.maxTimestamp = 0
}

func (qo *queryOptions) copyFrom(other *queryOptions) {
	qo.StreamQueryOptions.CopyFrom(&other.StreamQueryOptions)
	qo.elementFilter = other.elementFilter
	qo.seriesToEntity = other.seriesToEntity
	qo.sortedSids = other.sortedSids
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
