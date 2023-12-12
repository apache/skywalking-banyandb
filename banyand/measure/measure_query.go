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
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
)

var errTagFamilyNotExist = errors.New("tag family doesn't exist")

// Query allow to retrieve measure data points.
type Query interface {
	LoadGroup(name string) (resourceSchema.Group, bool)
	Measure(measure *commonv1.Metadata) (Measure, error)
}

// Measure allows inspecting measure data points' details.
type Measure interface {
	io.Closer
	Query(ctx context.Context, opts pbv1.MeasureQueryOptions) (pbv1.MeasureQueryResult, error)
	Shards(entity tsdb.Entity) ([]tsdb.Shard, error)
	CompanionShards(metadata *commonv1.Metadata) ([]tsdb.Shard, error)
	Shard(id common.ShardID) (tsdb.Shard, error)
	ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error)
	ParseField(name string, item tsdb.Item) (*measurev1.DataPoint_Field, error)
	GetSchema() *databasev1.Measure
	GetIndexRules() []*databasev1.IndexRule
	GetInterval() time.Duration
	GetShardNum() uint32
	SetSchema(schema *databasev1.Measure)
}

var _ Measure = (*measure)(nil)

func (s *measure) SetSchema(schema *databasev1.Measure) {
	s.schema = schema
}

func (s *measure) GetInterval() time.Duration {
	return s.interval
}

func (s *measure) GetShardNum() uint32 {
	return s.shardNum
}

type QueryResult struct {
	data           []*blockCursor
	originalData   []*blockCursor
	lastIndex      int
	pws            []*partWrapper
	shouldMergeAll bool
	asc            bool
}

const round = time.Millisecond

func (qr *QueryResult) Pull() *pbv1.Result {
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
	}
	if qr.shouldMergeAll {
		if len(qr.data) == 1 {
			r := &pbv1.Result{}
			bc := qr.data[0]
			bc.copyAllTo(r)
			return r
		}
		return qr.merge(round)
	}
	originalData := qr.data
	var lastSeriesID common.SeriesID
	for i := qr.lastIndex; i < len(originalData); i++ {
		bc := originalData[i]
		if lastSeriesID != 0 && lastSeriesID != bc.bm.seriesID {
			qr.data = qr.data[:0]
			qr.data = append(qr.data, originalData[qr.lastIndex:i]...)
			qr.lastIndex = i
			return qr.merge(round)
		}
		lastSeriesID = bc.bm.seriesID
	}
	if qr.lastIndex < len(originalData)-1 {
		qr.data = originalData[qr.lastIndex:]
		return qr.merge(round)
	}
	return nil
}

func (qr *QueryResult) Release() {
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

func (qr QueryResult) Len() int {
	return len(qr.data)
}

func (qr QueryResult) Less(i, j int) bool {
	if qr.asc {
		return qr.data[i].timestamps[qr.data[i].idx] < qr.data[j].timestamps[qr.data[j].idx]
	}
	return qr.data[i].timestamps[qr.data[i].idx] > qr.data[j].timestamps[qr.data[j].idx]
}

func (qr QueryResult) Swap(i, j int) {
	qr.data[i], qr.data[j] = qr.data[j], qr.data[i]
}

func (qr *QueryResult) Push(x interface{}) {
	qr.data = append(qr.data, x.(*blockCursor))
}

func (qr *QueryResult) Pop() interface{} {
	old := qr.data
	n := len(old)
	x := old[n-1]
	qr.data = old[0 : n-1]
	return x
}

func (qr *QueryResult) merge(round time.Duration) *pbv1.Result {
	result := &pbv1.Result{}
	var lastOriginalTimestamp int64
	r := int64(round)

	for qr.Len() > 0 {
		bc := heap.Pop(qr).(*blockCursor)

		roundedTimestamp := (bc.timestamps[bc.idx] / r) * r

		if len(result.Timestamps) > 0 && roundedTimestamp == result.Timestamps[len(result.Timestamps)-1] {
			if bc.timestamps[bc.idx] > lastOriginalTimestamp {
				lastOriginalTimestamp = bc.timestamps[bc.idx]
			}
		} else {
			bc.copyTo(result)
			lastOriginalTimestamp = bc.timestamps[bc.idx]
		}

		bc.idx++

		if bc.idx < len(bc.timestamps) {
			heap.Push(qr, bc)
		}
	}

	return result
}

type QueryOptions struct {
	pbv1.MeasureQueryOptions
	minTimestamp int64
	maxTimestamp int64
}

func (s *measure) Query(ctx context.Context, mqo pbv1.MeasureQueryOptions) (pbv1.MeasureQueryResult, error) {
	if mqo.TimeRange == nil || mqo.Entity == nil {
		return nil, errors.New("invalid query options: timeRange and series are required")
	}
	tsdb := s.databaseSupplier.SupplyTSDB().(storage.TSDB[*tsTable])
	tabWrappers := tsdb.SelectTSTables(*mqo.TimeRange)
	sl, err := tsdb.IndexDB().Search(ctx, &pbv1.Series{Subject: mqo.Name, EntityValues: mqo.Entity}, mqo.Filter, mqo.Order)
	if err != nil {
		return nil, err
	}
	var sids []common.SeriesID
	for i := range sl {
		sids = append(sids, sl[i].ID)
	}
	var pws []*partWrapper
	var parts []*part
	qo := &QueryOptions{
		MeasureQueryOptions: mqo,
		minTimestamp:        mqo.TimeRange.Start.UnixNano(),
		maxTimestamp:        mqo.TimeRange.End.UnixNano(),
	}
	for _, tw := range tabWrappers {
		pws, parts = tw.Table().getParts(pws, parts, qo)
	}
	// TODO: cache tstIter
	var tstIter tstIter
	originalSids := make([]common.SeriesID, len(sids))
	copy(originalSids, sids)
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	tstIter.init(parts, sids, qo)
	if tstIter.err != nil {
		return nil, fmt.Errorf("cannot init tstIter: %w", tstIter.err)
	}
	var result QueryResult
	for tstIter.NextBlock() {
		bc := generateBlockCursor()
		p := tstIter.piHeap[0]
		bc.init(p.p, p.bm, qo)
		result.data = append(result.data, bc)
	}
	if mqo.Order != nil {
		if mqo.Order.Sort == modelv1.Sort_SORT_ASC || mqo.Order.Sort == modelv1.Sort_SORT_UNSPECIFIED {
			result.asc = true
		}
		sidToIndex := make(map[common.SeriesID]int)
		for i, si := range originalSids {
			sidToIndex[si] = i
		}
		sort.Slice(result.data, func(i, j int) bool {
			return sidToIndex[result.data[i].bm.seriesID] < sidToIndex[result.data[j].bm.seriesID]
		})
	} else {
		result.asc = true
	}
	return &result, nil
}

type tableIterator struct {
	sids     []common.SeriesID
	tsTables []*tsTable
}

func (si *tableIterator) reset() {
}

// func (si *tableIterator) init(seriesTablesList []*seriesTables) {

// }

func (si *tableIterator) nextBlock() bool {
	return false
}

func generateTableIterator() *tableIterator {
	v := tiPool.Get()
	if v == nil {
		return &tableIterator{}
	}
	return v.(*tableIterator)
}

func releaseTableIterator(ti *tableIterator) {
	ti.reset()
	tiPool.Put(ti)
}

var tiPool sync.Pool

func (bc *blockCursor) copyAllTo(r *pbv1.Result) {
	r.Timestamps = bc.timestamps
	for _, cf := range bc.tagFamilies {
		tf := pbv1.TagFamily{
			Name: cf.Name,
		}
		for _, c := range cf.Columns {
			t := pbv1.Tag{
				Name: c.Name,
			}
			for _, v := range c.Values {
				t.Values = append(t.Values, mustDecodeTagValue(c.ValueType, v))
			}
			tf.Tags = append(tf.Tags, t)
		}
		r.TagFamilies = append(r.TagFamilies, tf)
	}
	for _, c := range bc.fields.Columns {
		f := pbv1.Field{
			Name: c.Name,
		}
		for _, v := range c.Values {
			f.Values = append(f.Values, mustDecodeFieldValue(c.ValueType, v))
		}
		r.Fields = append(r.Fields, f)
	}
}

func (bc *blockCursor) copyTo(r *pbv1.Result) {
	r.Timestamps = append(r.Timestamps, bc.timestamps[bc.idx])
	if len(r.TagFamilies) != len(bc.tagFamilies) {
		for _, cf := range bc.tagFamilies {
			tf := pbv1.TagFamily{
				Name: cf.Name,
			}
			for _, c := range cf.Columns {
				t := pbv1.Tag{
					Name: c.Name,
				}
				tf.Tags = append(tf.Tags, t)
			}
			r.TagFamilies = append(r.TagFamilies, tf)
		}
	}
	for i, cf := range bc.tagFamilies {
		for i2, c := range cf.Columns {
			r.TagFamilies[i].Tags[i2].Values = append(r.TagFamilies[i].Tags[i2].Values, mustDecodeTagValue(c.ValueType, c.Values[bc.idx]))
		}
	}

	if len(r.Fields) != len(bc.fields.Columns) {
		for _, c := range bc.fields.Columns {
			f := pbv1.Field{
				Name: c.Name,
			}
			r.Fields = append(r.Fields, f)
		}
	}
	for i, c := range bc.fields.Columns {
		r.Fields[i].Values = append(r.Fields[i].Values, mustDecodeFieldValue(c.ValueType, c.Values[bc.idx]))
	}
}

func mustDecodeTagValue(valueType pbv1.ValueType, value []byte) *modelv1.TagValue {
	switch valueType {
	case pbv1.ValueTypeInt64:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Int{
				Int: &modelv1.Int{
					Value: convert.BytesToInt64(value),
				},
			},
		}
	case pbv1.ValueTypeStr:
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_Str{
				Str: &modelv1.Str{
					Value: string(value),
				},
			},
		}
	case pbv1.ValueTypeBinaryData:
		data := make([]byte, len(value))
		copy(data, value)
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_BinaryData{
				BinaryData: data,
			},
		}
	case pbv1.ValueTypeInt64Arr:
		var values []int64
		for i := 0; i < len(value); i += 8 {
			values = append(values, convert.BytesToInt64(value[i:i+8]))
		}
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_IntArray{
				IntArray: &modelv1.IntArray{
					Value: values,
				},
			},
		}
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
		return &modelv1.TagValue{
			Value: &modelv1.TagValue_StrArray{
				StrArray: &modelv1.StrArray{
					Value: values,
				},
			},
		}
	default:
		logger.Panicf("unsupported value type: %v", valueType)
		return nil
	}
}

func mustDecodeFieldValue(valueType pbv1.ValueType, value []byte) *modelv1.FieldValue {
	switch valueType {
	case pbv1.ValueTypeInt64:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: convert.BytesToInt64(value),
				},
			},
		}
	case pbv1.ValueTypeFloat64:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Float{
				Float: &modelv1.Float{
					Value: convert.BytesToFloat64(value),
				},
			},
		}
	case pbv1.ValueTypeStr:
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Str{
				Str: &modelv1.Str{
					Value: string(value),
				},
			},
		}
	case pbv1.ValueTypeBinaryData:
		data := make([]byte, len(value))
		copy(data, value)
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_BinaryData{
				BinaryData: data,
			},
		}
	default:
		logger.Panicf("unsupported value type: %v", valueType)
		return nil
	}
}

func (s *measure) Shards(entity tsdb.Entity) ([]tsdb.Shard, error) {
	panic("implement me")
	// wrap := func(shards []tsdb.Shard) []tsdb.Shard {
	// 	result := make([]tsdb.Shard, len(shards))
	// 	for i := 0; i < len(shards); i++ {
	// 		result[i] = tsdb.NewScopedShard(tsdb.Entry(s.name), shards[i])
	// 	}
	// 	return result
	// }
	// db := s.databaseSupplier.SupplyTSDB()
	// if len(entity) < 1 {
	// 	return wrap(db.Shards()), nil
	// }
	// for _, e := range entity {
	// 	if e == nil {
	// 		return wrap(db.Shards()), nil
	// 	}
	// }
	// shardID, err := partition.ShardID(entity.Prepend(tsdb.Entry(s.name)).Marshal(), s.shardNum)
	// if err != nil {
	// 	return nil, err
	// }
	// shard, err := s.databaseSupplier.SupplyTSDB().Shard(common.ShardID(shardID))
	// if err != nil {
	// 	if errors.Is(err, tsdb.ErrUnknownShard) {
	// 		return []tsdb.Shard{}, nil
	// 	}
	// 	return nil, err
	// }
	// return []tsdb.Shard{tsdb.NewScopedShard(tsdb.Entry(s.name), shard)}, nil
}

func (s *measure) CompanionShards(metadata *commonv1.Metadata) ([]tsdb.Shard, error) {
	panic("implement me")
	// wrap := func(shards []tsdb.Shard) []tsdb.Shard {
	// 	result := make([]tsdb.Shard, len(shards))
	// 	for i := 0; i < len(shards); i++ {
	// 		result[i] = tsdb.NewScopedShard(tsdb.Entry(formatMeasureCompanionPrefix(s.name, metadata.GetName())), shards[i])
	// 	}
	// 	return result
	// }
	// db := s.databaseSupplier.SupplyTSDB()
	// return wrap(db.Shards()), nil
}

func (s *measure) Shard(id common.ShardID) (tsdb.Shard, error) {
	panic("implement me")
	// shard, err := s.databaseSupplier.SupplyTSDB().Shard(id)
	// if err != nil {
	// 	return nil, err
	// }
	// return tsdb.NewScopedShard(tsdb.Entry(s.name), shard), nil
}

func (s *measure) ParseTagFamily(family string, item tsdb.Item) (*modelv1.TagFamily, error) {
	fid := familyIdentity(family, pbv1.TagFlag)
	familyRawBytes, err := item.Family(fid)
	if err != nil {
		return nil, errors.Wrapf(err, "measure %s.%s parse family %s", s.name, s.group, family)
	}
	tagFamily := &modelv1.TagFamilyForWrite{}
	err = proto.Unmarshal(familyRawBytes, tagFamily)
	if err != nil {
		return nil, err
	}
	tags := make([]*modelv1.Tag, len(tagFamily.GetTags()))
	var tagSpec []*databasev1.TagSpec
	for _, tf := range s.schema.GetTagFamilies() {
		if tf.GetName() == family {
			tagSpec = tf.GetTags()
		}
	}
	if tagSpec == nil {
		return nil, errTagFamilyNotExist
	}
	for i, tag := range tagFamily.GetTags() {
		tags[i] = &modelv1.Tag{
			Key: tagSpec[i].GetName(),
			Value: &modelv1.TagValue{
				Value: tag.GetValue(),
			},
		}
	}
	return &modelv1.TagFamily{
		Name: family,
		Tags: tags,
	}, err
}

func (s *measure) ParseField(name string, item tsdb.Item) (*measurev1.DataPoint_Field, error) {
	var fieldSpec *databasev1.FieldSpec
	for _, spec := range s.schema.GetFields() {
		if spec.GetName() == name {
			fieldSpec = spec
			break
		}
	}
	fid := familyIdentity(name, pbv1.EncoderFieldFlag(fieldSpec, s.interval))
	bytes, err := item.Family(fid)
	if err != nil {
		return nil, errors.Wrapf(err, "measure %s.%s parse field %s", s.name, s.group, name)
	}
	fieldValue, err := pbv1.DecodeFieldValue(bytes, fieldSpec)
	if err != nil {
		return nil, err
	}
	return &measurev1.DataPoint_Field{
		Name:  name,
		Value: fieldValue,
	}, err
}
