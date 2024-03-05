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

package measure

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// Query allow to retrieve measure data points.
type Query interface {
	LoadGroup(name string) (resourceSchema.Group, bool)
	Measure(measure *commonv1.Metadata) (Measure, error)
}

// Measure allows inspecting measure data points' details.
type Measure interface {
	io.Closer
	Query(ctx context.Context, opts pbv1.MeasureQueryOptions) (pbv1.MeasureQueryResult, error)
	GetSchema() *databasev1.Measure
	GetIndexRules() []*databasev1.IndexRule
	SetSchema(schema *databasev1.Measure)
}

var _ Measure = (*measure)(nil)

func (s *measure) SetSchema(schema *databasev1.Measure) {
	s.schema = schema
}

type queryOptions struct {
	pbv1.MeasureQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func (s *measure) Query(ctx context.Context, mqo pbv1.MeasureQueryOptions) (pbv1.MeasureQueryResult, error) {
	if mqo.TimeRange == nil || mqo.Entity == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	if len(mqo.TagProjection) == 0 && len(mqo.FieldProjection) == 0 {
		return nil, errors.New("invalid query options: tagProjection or fieldProjection is required")
	}
	tsdb := s.databaseSupplier.SupplyTSDB().(storage.TSDB[*tsTable, option])
	tabWrappers := tsdb.SelectTSTables(*mqo.TimeRange)
	defer func() {
		for i := range tabWrappers {
			tabWrappers[i].DecRef()
		}
	}()

	sl, err := tsdb.IndexDB().Search(ctx, &pbv1.Series{Subject: mqo.Name, EntityValues: mqo.Entity}, mqo.Filter, mqo.Order)
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
	var parts []*part
	qo := queryOptions{
		MeasureQueryOptions: mqo,
		minTimestamp:        mqo.TimeRange.Start.UnixNano(),
		maxTimestamp:        mqo.TimeRange.End.UnixNano(),
	}
	var n int
	for i := range tabWrappers {
		s := tabWrappers[i].Table().currentSnapshot()
		if s == nil {
			continue
		}
		parts, n = s.getParts(parts, qo.minTimestamp, qo.maxTimestamp)
		if n < 1 {
			s.decRef()
			continue
		}
		result.snapshots = append(result.snapshots, s)
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
	projectedEntityOffsets, tagProjectionOnPart := s.parseTagProjection(qo, &result)
	result.tagProjection = qo.TagProjection
	qo.TagProjection = tagProjectionOnPart
	for tstIter.nextBlock() {
		bc := generateBlockCursor()
		p := tstIter.piHeap[0]

		seriesID := p.curBlock.seriesID
		if result.entityValues != nil && result.entityValues[seriesID] == nil {
			for i := range sl {
				if sl[i].ID == seriesID {
					tag := make(map[string]*modelv1.TagValue)
					for name, offset := range projectedEntityOffsets {
						tag[name] = sl[i].EntityValues[offset]
					}
					result.entityValues[seriesID] = tag
				}
			}
		}
		bc.init(p.p, p.curBlock, qo)
		result.data = append(result.data, bc)
	}
	if tstIter.Error() != nil {
		return nil, fmt.Errorf("cannot iterate tstIter: %w", tstIter.Error())
	}

	result.sidToIndex = make(map[common.SeriesID]int)
	for i, si := range originalSids {
		result.sidToIndex[si] = i
	}
	if mqo.Order == nil {
		result.orderByTS = true
		result.ascTS = true
		return &result, nil
	}
	if mqo.Order.Index == nil {
		result.orderByTS = true
		if mqo.Order.Sort == modelv1.Sort_SORT_ASC || mqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.ascTS = true
		}
		return &result, nil
	}

	return &result, nil
}

func (s *measure) parseTagProjection(qo queryOptions, result *queryResult) (projectedEntityOffsets map[string]int, tagProjectionOnPart []pbv1.TagProjection) {
	projectedEntityOffsets = make(map[string]int)
	for i := range qo.TagProjection {
		var found bool
		for j := range qo.TagProjection[i].Names {
			for k := range s.schema.GetEntity().GetTagNames() {
				if qo.TagProjection[i].Names[j] == s.schema.GetEntity().GetTagNames()[k] {
					projectedEntityOffsets[qo.TagProjection[i].Names[j]] = k
					if result.entityValues == nil {
						result.entityValues = make(map[common.SeriesID]map[string]*modelv1.TagValue)
					}
				} else {
					if !found {
						found = true
						tagProjectionOnPart = append(tagProjectionOnPart, pbv1.TagProjection{
							Family: qo.TagProjection[i].Family,
						})
					}
					tagProjectionOnPart[len(tagProjectionOnPart)-1].Names = append(
						tagProjectionOnPart[len(tagProjectionOnPart)-1].Names,
						qo.TagProjection[i].Names[j])
				}
			}
		}
	}
	return
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
	sidToIndex    map[common.SeriesID]int
	entityValues  map[common.SeriesID]map[string]*modelv1.TagValue
	tagProjection []pbv1.TagProjection
	data          []*blockCursor
	snapshots     []*snapshot
	loaded        bool
	orderByTS     bool
	ascTS         bool
}

func (qr *queryResult) Pull() *pbv1.MeasureResult {
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
			if i < 0 {
				continue
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
		r := &pbv1.MeasureResult{}
		bc := qr.data[0]
		bc.copyAllTo(r, qr.entityValues, qr.tagProjection, qr.orderByTimestampDesc())
		qr.data = qr.data[:0]
		return r
	}
	return qr.merge(qr.entityValues, qr.tagProjection)
}

func (qr *queryResult) Release() {
	for i, v := range qr.data {
		releaseBlockCursor(v)
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
	for i := range qr.snapshots {
		qr.snapshots[i].decRef()
	}
	qr.snapshots = qr.snapshots[:0]
}

func (qr queryResult) Len() int {
	return len(qr.data)
}

func (qr queryResult) Less(i, j int) bool {
	leftTS := qr.data[i].timestamps[qr.data[i].idx]
	rightTS := qr.data[j].timestamps[qr.data[j].idx]
	leftVersion := qr.data[i].p.partMetadata.ID
	rightVersion := qr.data[j].p.partMetadata.ID
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

func (qr *queryResult) merge(entityValuesAll map[common.SeriesID]map[string]*modelv1.TagValue,
	tagProjection []pbv1.TagProjection,
) *pbv1.MeasureResult {
	step := 1
	if qr.orderByTimestampDesc() {
		step = -1
	}
	result := &pbv1.MeasureResult{}
	var lastPartVersion uint64
	var lastSid common.SeriesID

	for qr.Len() > 0 {
		topBC := qr.data[0]
		if lastSid != 0 && topBC.bm.seriesID != lastSid {
			return result
		}
		lastSid = topBC.bm.seriesID

		if len(result.Timestamps) > 0 &&
			topBC.timestamps[topBC.idx] == result.Timestamps[len(result.Timestamps)-1] {
			if topBC.p.partMetadata.ID > lastPartVersion {
				logger.Panicf("following parts version should be less or equal to the previous one")
			}
		} else {
			topBC.copyTo(result, entityValuesAll, tagProjection)
			lastPartVersion = topBC.p.partMetadata.ID
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
