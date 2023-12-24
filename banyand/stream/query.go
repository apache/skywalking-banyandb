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
// software distributed under the License is distributed on ae
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"container/heap"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Query allow to retrieve stream elements.
// type Query interface {
// 	LoadGroup(name string) (resourceSchema.Group, bool)
// 	Stream(stream *commonv1.Metadata) (Stream, error)
// }

// // Stream allows inspecting stream elements' details.
// type Stream interface {
// 	io.Closer
// 	Query(ctx context.Context, opts pbv1.StreamQueryOptions) ([]*streamv1.Element, error)
// 	GetSchema() *databasev1.Stream
// 	GetIndexRules() []*databasev1.IndexRule
// 	SetSchema(schema *databasev1.Stream)

// 	// Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
// 	// Shard(id common.ShardID) (tsdb.Shard, error)
// 	// ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error)
// 	ParseElementID(item tsdb.Item) (string, error)
// }

// func (s *stream) Shards(entity tsdb.Entity) ([]tsdb.Shard, error) {
// 	wrap := func(shards []tsdb.Shard) []tsdb.Shard {
// 		result := make([]tsdb.Shard, len(shards))
// 		for i := 0; i < len(shards); i++ {
// 			result[i] = tsdb.NewScopedShard(tsdb.Entry(s.name), shards[i])
// 		}
// 		return result
// 	}
// 	db := s.db.SupplyTSDB()
// 	if len(entity) < 1 {
// 		return wrap(db.Shards()), nil
// 	}
// 	for _, e := range entity {
// 		if e == nil {
// 			return wrap(db.Shards()), nil
// 		}
// 	}
// 	shardID, err := partition.ShardID(entity.Prepend(tsdb.Entry(s.name)).Marshal(), s.shardNum)
// 	if err != nil {
// 		return nil, err
// 	}
// 	shard, err := db.Shard(common.ShardID(shardID))
// 	if err != nil {
// 		if errors.Is(err, tsdb.ErrUnknownShard) {
// 			return []tsdb.Shard{}, nil
// 		}
// 		return nil, err
// 	}
// 	return []tsdb.Shard{tsdb.NewScopedShard(tsdb.Entry(s.name), shard)}, nil
// }

// func (s *stream) Shard(id common.ShardID) (tsdb.Shard, error) {
// 	shard, err := s.db.SupplyTSDB().Shard(id)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return tsdb.NewScopedShard(tsdb.Entry(s.name), shard), nil
// }

// func (s *stream) ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error) {
// 	familyRawBytes, err := item.Family(tsdb.Hash([]byte(family)))
// 	if err != nil {
// 		return nil, errors.Wrapf(err, "stream %s.%s parse family %s", s.name, s.group, family)
// 	}
// 	tagFamily := &modelv1.TagFamilyForWrite{}
// 	err = proto.Unmarshal(familyRawBytes, tagFamily)
// 	if err != nil {
// 		return nil, err
// 	}
// 	tags := make([]*modelv1.Tag, len(tagFamily.GetTags()))
// 	var tagSpec []*databasev1.TagSpec
// 	for _, tf := range s.schema.GetTagFamilies() {
// 		if tf.GetName() == family {
// 			tagSpec = tf.GetTags()
// 		}
// 	}
// 	if tagSpec == nil {
// 		return nil, errTagFamilyNotExist
// 	}
// 	for i, tag := range tagFamily.GetTags() {
// 		tags[i] = &modelv1.Tag{
// 			Key: tagSpec[i].GetName(),
// 			Value: &modelv1.TagValue{
// 				Value: tag.GetValue(),
// 			},
// 		}
// 	}
// 	return &modelv1.TagFamily{
// 		Name: family,
// 		Tags: tags,
// 	}, err
// }

// func (s *stream) ParseElementID(item tsdb.Item) (string, error) {
// 	rawBytes, err := item.Val()
// 	if err != nil {
// 		return "", err
// 	}
// 	return string(rawBytes), nil
// }

// var _ Stream = (*stream)(nil)

// func (s *stream) SetSchema(schema *databasev1.Stream) {
// 	s.schema = schema
// }

type queryOptions struct {
	pbv1.StreamQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func mustDecodeTagValue(valueType pbv1.ValueType, value []byte) *modelv1.TagValue {
	if value == nil {
		switch valueType {
		case pbv1.ValueTypeInt64:
			logger.Panicf("int64 can be nil")
		case pbv1.ValueTypeStr:
			return pbv1.EmptyStrTagValue
		case pbv1.ValueTypeStrArr:
			return pbv1.EmptyStrArrTagValue
		case pbv1.ValueTypeInt64Arr:
			return pbv1.EmptyIntArrTagValue
		case pbv1.ValueTypeBinaryData:
			return pbv1.EmptyBinaryTagValue
		default:
			return pbv1.NullTagValue
		}
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

func mustDecodeFieldValue(valueType pbv1.ValueType, value []byte) *modelv1.FieldValue {
	if value == nil {
		switch valueType {
		case pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64:
			logger.Panicf("int64 and float64 can't be nil")
		case pbv1.ValueTypeStr:
			return pbv1.EmptyStrFieldValue
		case pbv1.ValueTypeBinaryData:
			return pbv1.EmptyBinaryFieldValue
		default:
			return pbv1.NullFieldValue
		}
	}
	switch valueType {
	case pbv1.ValueTypeInt64:
		return int64FieldValue(convert.BytesToInt64(value))
	case pbv1.ValueTypeFloat64:
		return float64FieldValue(convert.BytesToFloat64(value))
	case pbv1.ValueTypeStr:
		return strFieldValue(string(value))
	case pbv1.ValueTypeBinaryData:
		return binaryDataFieldValue(value)
	default:
		logger.Panicf("unsupported value type: %v", valueType)
		return nil
	}
}

func int64FieldValue(value int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Int{
			Int: &modelv1.Int{
				Value: value,
			},
		},
	}
}

func float64FieldValue(value float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Float{
			Float: &modelv1.Float{
				Value: value,
			},
		},
	}
}

func strFieldValue(value string) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Str{
			Str: &modelv1.Str{
				Value: value,
			},
		},
	}
}

func binaryDataFieldValue(value []byte) *modelv1.FieldValue {
	data := make([]byte, len(value))
	copy(data, value)
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_BinaryData{
			BinaryData: data,
		},
	}
}

type queryResult struct {
	sidToIndex map[common.SeriesID]int
	data       []*blockCursor
	pws        []*partWrapper
	loaded     bool
	orderByTS  bool
	ascTS      bool
}

func (qr *queryResult) Pull() *pbv1.Result {
	if !qr.loaded {
		if len(qr.data) == 0 {
			return nil
		}
		// TODO:// Parallel load
		tmpBlock := generateBlock()
		defer releaseBlock(tmpBlock)
		for i := 0; i < len(qr.data); i++ {
			if !qr.data[i].loadData(tmpBlock) {
				qr.data = append(qr.data[:i], qr.data[i+1:]...)
				i--
			}
			if qr.orderByTimestampDesc() {
				qr.data[i].idx = len(qr.data[i].timestamps) - 1
			}
		}
		qr.loaded = true
		heap.Init(qr)
	}
	if len(qr.data) == 0 {
		return nil
	}
	if len(qr.data) == 1 {
		r := &pbv1.Result{}
		bc := qr.data[0]
		bc.copyAllTo(r)
		qr.data = qr.data[:0]
		return r
	}
	return qr.merge()
}

func (qr *queryResult) Release() {
	for i, v := range qr.data {
		releaseBlockCursor(v)
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
	for i := range qr.pws {
		qr.pws[i].decRef()
	}
	qr.pws = qr.pws[:0]
}

func (qr queryResult) Len() int {
	return len(qr.data)
}

func (qr queryResult) Less(i, j int) bool {
	leftTS := qr.data[i].timestamps[qr.data[i].idx]
	rightTS := qr.data[j].timestamps[qr.data[j].idx]
	leftVersion := qr.data[i].p.partMetadata.Version
	rightVersion := qr.data[j].p.partMetadata.Version
	if qr.orderByTS {
		if leftTS == rightTS {
			if qr.data[i].bm.seriesID == qr.data[j].bm.seriesID {
				// sort version in descending order if timestamps and seriesID are equal
				return leftVersion > rightVersion
			}
			// sort seriesID in ascending order if timestamps are equal
			return qr.data[i].bm.seriesID < qr.data[j].bm.seriesID
		}
		if qr.ascTS {
			return leftTS < rightTS
		}
		return leftTS > rightTS
	}
	leftSIDIndex := qr.sidToIndex[qr.data[i].bm.seriesID]
	rightSIDIndex := qr.sidToIndex[qr.data[j].bm.seriesID]
	if leftSIDIndex == rightSIDIndex {
		if leftTS == rightTS {
			// sort version in descending order if timestamps and seriesID are equal
			return leftVersion > rightVersion
		}
		// sort timestamps in ascending order if seriesID are equal
		return leftTS < rightTS
	}
	return leftSIDIndex < rightSIDIndex
}

func (qr queryResult) Swap(i, j int) {
	qr.data[i], qr.data[j] = qr.data[j], qr.data[i]
}

func (qr *queryResult) Push(x interface{}) {
	qr.data = append(qr.data, x.(*blockCursor))
}

func (qr *queryResult) Pop() interface{} {
	old := qr.data
	n := len(old)
	x := old[n-1]
	qr.data = old[0 : n-1]
	return x
}

func (qr *queryResult) orderByTimestampDesc() bool {
	return qr.orderByTS && !qr.ascTS
}

func (qr *queryResult) merge() *pbv1.Result {
	step := 1
	if qr.orderByTimestampDesc() {
		step = -1
	}
	result := &pbv1.Result{}
	var lastPartVersion int64
	var lastSid common.SeriesID

	for qr.Len() > 0 {
		topBC := qr.data[0]
		if lastSid != 0 && topBC.bm.seriesID != lastSid {
			return result
		}
		lastSid = topBC.bm.seriesID

		if len(result.Timestamps) > 0 &&
			topBC.timestamps[topBC.idx] == result.Timestamps[len(result.Timestamps)-1] {
			if topBC.p.partMetadata.Version > lastPartVersion {
				logger.Panicf("following parts version should be less or equal to the previous one")
			}
		} else {
			topBC.copyTo(result)
			lastPartVersion = topBC.p.partMetadata.Version
		}

		topBC.idx += step

		if qr.orderByTimestampDesc() {
			if topBC.idx < 0 {
				heap.Pop(qr)
			} else {
				heap.Fix(qr, 0)
			}
		} else {
			if topBC.idx >= len(topBC.timestamps) {
				heap.Pop(qr)
			} else {
				heap.Fix(qr, 0)
			}
		}
	}

	return result
}
