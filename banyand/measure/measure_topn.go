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
	"context"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	timeBucketFormat = "200601021504"
	TopNTagFamily    = "__topN__"
)

var (
	_ bus.MessageListener = (*topNProcessCallback)(nil)
	_ io.Closer           = (*topNStreamingProcessor)(nil)
	_ io.Closer           = (*topNProcessorManager)(nil)
	_ flow.Sink           = (*topNStreamingProcessor)(nil)

	TopNValueFieldSpec = &databasev1.FieldSpec{
		Name:              "value",
		FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}
)

type topNStreamingProcessor struct {
	flow.ComponentState
	l                *logger.Logger
	shardNum         uint32
	interval         time.Duration
	slideSize        time.Duration
	topNSchema       *databasev1.TopNAggregation
	databaseSupplier tsdb.Supplier
	src              chan interface{}
	in               chan flow.StreamRecord
	errCh            <-chan error
	stopCh           chan interface{}
	streamingFlow    flow.Flow
}

func (t *topNStreamingProcessor) In() chan<- flow.StreamRecord {
	return t.in
}

func (t *topNStreamingProcessor) Setup(ctx context.Context) error {
	go t.run(ctx)
	return nil
}

func (t *topNStreamingProcessor) run(ctx context.Context) {
	t.Add(1)
	defer t.Done()
	for {
		select {
		case record, ok := <-t.in:
			if !ok {
				return
			}
			if err := t.writeStreamRecord(record); err != nil {
				t.l.Err(err).Msg("fail to write stream record")
			}
		case <-ctx.Done():
			return
		}
	}
}

// Teardown is called by the Flow as a lifecycle hook.
// So we should not block on err channel within this method.
func (t *topNStreamingProcessor) Teardown(ctx context.Context) error {
	t.Wait()
	return nil
}

func (t *topNStreamingProcessor) Close() error {
	close(t.src)
	defer close(t.stopCh)
	return t.streamingFlow.Close()
}

func (t *topNStreamingProcessor) writeStreamRecord(record flow.StreamRecord) error {
	tuples, ok := record.Data().([]*streaming.Tuple2)
	if !ok {
		return errors.New("invalid data type")
	}
	// eventTime is the start time of a timeWindow
	eventTime := time.UnixMilli(record.TimestampMillis())
	// down-sampling to a time bucket as measure ID
	timeBucket := t.downSampleTimeBucket(eventTime)
	var err error
	for rankNum, tuple := range tuples {
		fieldValue := tuple.V1.(int64)
		data := tuple.V2.(flow.Data)
		err = multierr.Append(err, t.writeData(eventTime, timeBucket, fieldValue, data, rankNum))
	}
	return err
}

func (t *topNStreamingProcessor) writeData(eventTime time.Time, timeBucket string, fieldValue int64, data flow.Data, rankNum int) error {
	var tagValues []*modelv1.TagValue
	if len(t.topNSchema.GetGroupByTagNames()) > 0 {
		var ok bool
		if tagValues, ok = data[2].([]*modelv1.TagValue); !ok {
			return errors.New("fail to extract tag values from topN result")
		}
	}
	entity, shardID, err := t.locate(tagValues, rankNum)
	if err != nil {
		return err
	}
	shard, err := t.databaseSupplier.SupplyTSDB().Shard(shardID)
	if err != nil {
		return err
	}
	series, err := shard.Series().GetByHashKey(tsdb.HashEntity(entity))
	if err != nil {
		return err
	}
	span, err := series.Span(timestamp.NewInclusiveTimeRangeDuration(eventTime, 0))
	if err != nil {
		if span != nil {
			_ = span.Close()
		}
		return err
	}
	// measureID is consist of three parts,
	// 1. groupValues
	// 2. rankNumber
	// 3. timeBucket
	measureID := data[0].(string) + "_" + strconv.Itoa(rankNum) + "_" + timeBucket
	writeFn := func() (tsdb.Writer, error) {
		builder := span.WriterBuilder().Time(eventTime)
		virtualTagFamily := &modelv1.TagFamilyForWrite{
			Tags: []*modelv1.TagValue{
				// MeasureID
				{
					Value: &modelv1.TagValue_Id{
						Id: &modelv1.ID{
							Value: measureID,
						},
					},
				},
				// GroupValues for merge in post processor
				{
					Value: &modelv1.TagValue_Str{
						Str: &modelv1.Str{
							Value: data[0].(string),
						},
					},
				},
			},
		}
		payload, errMarshal := proto.Marshal(virtualTagFamily)
		if errMarshal != nil {
			return nil, errMarshal
		}
		builder.Family(familyIdentity(TopNTagFamily, TagFlag), payload)
		virtualFieldValue := &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: fieldValue,
				},
			},
		}
		fieldData := encodeFieldValue(virtualFieldValue)
		builder.Family(familyIdentity(TopNValueFieldSpec.GetName(), EncoderFieldFlag(TopNValueFieldSpec, t.interval)), fieldData)
		writer, errWrite := builder.Build()
		if errWrite != nil {
			return nil, errWrite
		}
		_, errWrite = writer.Write()
		t.l.Debug().
			Time("ts", eventTime).
			Int("ts_nano", eventTime.Nanosecond()).
			Uint64("series_id", uint64(series.ID())).
			Uint64("item_id", uint64(writer.ItemID().ID)).
			Int("shard_id", int(shardID)).
			Msg("write measure")
		return writer, errWrite
	}
	_, err = writeFn()
	if err != nil {
		_ = span.Close()
		return err
	}
	return span.Close()
}

func (t *topNStreamingProcessor) downSampleTimeBucket(eventTime time.Time) string {
	return time.UnixMilli(eventTime.UnixMilli() - eventTime.UnixMilli()%t.interval.Milliseconds()).
		Format(timeBucketFormat)
}

func (t *topNStreamingProcessor) locate(tagValues []*modelv1.TagValue, rankNum int) (tsdb.Entity, common.ShardID, error) {
	if len(t.topNSchema.GetGroupByTagNames()) != len(tagValues) {
		return nil, 0, errors.New("no enough tag values for the entity")
	}
	entity := make(tsdb.Entity, 1+1+len(t.topNSchema.GetGroupByTagNames()))
	// entity prefix
	entity[0] = []byte(formatMeasureCompanionPrefix(t.topNSchema.GetSourceMeasure().GetName(),
		t.topNSchema.GetMetadata().GetName()))
	entity[1] = convert.Int64ToBytes(int64(rankNum))
	// measureID as sharding key
	for idx, tagVal := range tagValues {
		var innerErr error
		entity[idx+2], innerErr = pbv1.MarshalIndexFieldValue(tagVal)
		if innerErr != nil {
			return nil, 0, innerErr
		}
	}
	id, err := partition.ShardID(entity.Marshal(), t.shardNum)
	if err != nil {
		return nil, 0, err
	}
	return entity, common.ShardID(id), nil
}

func (t *topNStreamingProcessor) start() *topNStreamingProcessor {
	t.errCh = t.streamingFlow.Window(streaming.NewSlidingTimeWindows(t.interval, t.slideSize)).
		TopN(int(t.topNSchema.GetCountersNumber()),
			streaming.WithSortKeyExtractor(func(elem interface{}) int64 {
				return elem.(flow.Data)[1].(int64)
			}),
			streaming.OrderBy(t.topNSchema.GetFieldValueSort()),
		).To(t).Open()
	go t.handleError()
	return t
}

func (t *topNStreamingProcessor) handleError() {
	for {
		select {
		case err, ok := <-t.errCh:
			if !ok {
				return
			}
			t.l.Err(err).Str("topN", t.topNSchema.GetMetadata().GetName()).
				Msg("error occurred during flow setup or process")
		case <-t.stopCh:
			return
		}
	}
}

// topNProcessorManager manages multiple topNStreamingProcessor(s) belonging to a single measure
type topNProcessorManager struct {
	l            *logger.Logger
	m            *measure
	topNSchemas  []*databasev1.TopNAggregation
	processorMap map[*commonv1.Metadata]*topNStreamingProcessor
}

func (manager *topNProcessorManager) Close() error {
	var err error
	for _, processor := range manager.processorMap {
		err = multierr.Append(err, processor.Close())
	}
	return err
}

func (manager *topNProcessorManager) onMeasureWrite(request *measurev1.WriteRequest) error {
	for _, processor := range manager.processorMap {
		processor.src <- flow.NewStreamRecordWithTimestampPb(request.GetDataPoint(), request.GetDataPoint().GetTimestamp())
	}

	return nil
}

func (manager *topNProcessorManager) start() error {
	interval := manager.m.interval
	// use 40% of the data point interval as the flush interval,
	// which means roughly the record located in the same time bucket will be persistent twice.
	// |---------------------------------|
	// |    40%     |    40%     |  20%  |
	// |          flush        flush     |
	// |---------------------------------|
	slideSize := time.Duration(float64(interval.Nanoseconds()) * 0.4)
	for _, topNSchema := range manager.topNSchemas {
		srcCh := make(chan interface{})
		src, _ := sources.NewChannel(srcCh)
		streamingFlow := streaming.New(src)

		if conditions := topNSchema.GetCriteria(); len(conditions) > 0 {
			filters, buildErr := manager.buildFilters(conditions)
			if buildErr != nil {
				return buildErr
			}
			streamingFlow = streamingFlow.Filter(filters)
		}

		mapper, innerErr := manager.buildMapper(topNSchema.GetFieldName(), topNSchema.GetGroupByTagNames()...)
		if innerErr != nil {
			return innerErr
		}
		streamingFlow = streamingFlow.Map(mapper)

		processor := &topNStreamingProcessor{
			l:                manager.l,
			shardNum:         manager.m.shardNum,
			interval:         interval,
			slideSize:        slideSize,
			topNSchema:       topNSchema,
			databaseSupplier: manager.m.databaseSupplier,
			src:              srcCh,
			in:               make(chan flow.StreamRecord),
			stopCh:           make(chan interface{}),
			streamingFlow:    streamingFlow,
		}

		manager.processorMap[topNSchema.GetSourceMeasure()] = processor.start()
	}

	return nil
}

func (manager *topNProcessorManager) buildFilters(criteria []*modelv1.Criteria) (flow.UnaryFunc[bool], error) {
	var filters []conditionFilter
	for _, group := range criteria {
		for _, cond := range group.GetConditions() {
			fIdx, tIdx, spec := pbv1.FindTagByName(manager.m.GetSchema().GetTagFamilies(), cond.GetName())
			if spec == nil {
				return nil, errors.New("fail to parse tag by name")
			}
			filters = append(filters, manager.buildFilterForTag(fIdx, tIdx, cond))
		}
	}

	if len(filters) == 0 {
		return func(_ context.Context, value interface{}) bool {
			return true
		}, nil
	}

	return func(_ context.Context, dataPoint any) bool {
		tfs := dataPoint.(*measurev1.DataPointValue).GetTagFamilies()
		for _, f := range filters {
			if !f.predicate(tfs) {
				return false
			}
		}
		return true
	}, nil
}

func (manager *topNProcessorManager) buildFilterForTag(familyOffset, tagOffset int, cond *modelv1.Condition) conditionFilter {
	switch v := cond.GetValue().GetValue().(type) {
	case *modelv1.TagValue_Int:
		return &int64TagFilter{
			TagLocator: partition.TagLocator{
				FamilyOffset: familyOffset,
				TagOffset:    tagOffset,
			},
			op:  cond.GetOp(),
			val: v.Int.GetValue(),
		}
	case *modelv1.TagValue_Str:
		return &strTagFilter{
			TagLocator: partition.TagLocator{
				FamilyOffset: familyOffset,
				TagOffset:    tagOffset,
			},
			op:  cond.GetOp(),
			val: v.Str.GetValue(),
		}
	case *modelv1.TagValue_Id:
		return &idTagFilter{
			TagLocator: partition.TagLocator{
				FamilyOffset: familyOffset,
				TagOffset:    tagOffset,
			},
			op:  cond.GetOp(),
			val: v.Id.GetValue(),
		}
	default:
		return nil
	}
}

func (manager *topNProcessorManager) buildMapper(fieldName string, groupByNames ...string) (flow.UnaryFunc[any], error) {
	fieldIdx := slices.IndexFunc(manager.m.GetSchema().GetFields(), func(spec *databasev1.FieldSpec) bool {
		return spec.GetName() == fieldName
	})
	if fieldIdx == -1 {
		return nil, errors.New("invalid fieldName")
	}
	if len(groupByNames) == 0 {
		return func(_ context.Context, request any) any {
			dataPoint := request.(*measurev1.DataPointValue)
			return flow.Data{
				// save string representation of group values as the key, i.e. v1
				"",
				// field value as v2
				// TODO: we only support int64
				dataPoint.GetFields()[fieldIdx].GetInt().GetValue(),
				// groupBy tag values as v3
				nil,
			}
		}, nil
	}
	groupLocator, err := newGroupLocator(manager.m.GetSchema(), groupByNames)
	if err != nil {
		return nil, err
	}
	return func(_ context.Context, request any) any {
		dataPoint := request.(*measurev1.DataPointValue)
		return flow.Data{
			// save string representation of group values as the key, i.e. v1
			strings.Join(transform(groupLocator, func(locator partition.TagLocator) string {
				return stringify(dataPoint.GetTagFamilies()[locator.FamilyOffset].GetTags()[locator.TagOffset])
			}), "|"),
			// field value as v2
			// TODO: we only support int64
			dataPoint.GetFields()[fieldIdx].GetInt().GetValue(),
			// groupBy tag values as v3
			transform(groupLocator, func(locator partition.TagLocator) *modelv1.TagValue {
				return dataPoint.GetTagFamilies()[locator.FamilyOffset].GetTags()[locator.TagOffset]
			}),
		}
	}, nil
}

// topNProcessCallback listens pipeline for writing requests
type topNProcessCallback struct {
	l          *logger.Logger
	schemaRepo *schemaRepo
}

func setUpStreamingProcessCallback(l *logger.Logger, schemaRepo *schemaRepo) bus.MessageListener {
	return &topNProcessCallback{
		l:          l,
		schemaRepo: schemaRepo,
	}
}

func (cb *topNProcessCallback) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(*measurev1.InternalWriteRequest)
	if !ok {
		cb.l.Warn().Msg("invalid event data type")
		return
	}

	// first get measure existence
	m, ok := cb.schemaRepo.loadMeasure(writeEvent.GetRequest().GetMetadata())
	if !ok {
		cb.l.Warn().Msg("cannot find measure definition")
		return
	}

	err := m.processorManager.onMeasureWrite(writeEvent.GetRequest())
	if err != nil {
		cb.l.Debug().Err(err).Msg("fail to send to the streaming processor")
	}

	return
}

var (
	_ conditionFilter = (*strTagFilter)(nil)
	_ conditionFilter = (*int64TagFilter)(nil)
)

type conditionFilter interface {
	predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool
}

type strTagFilter struct {
	partition.TagLocator
	op  modelv1.Condition_BinaryOp
	val string
}

func (f *strTagFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	strValue := tagFamilies[f.FamilyOffset].GetTags()[f.TagOffset].GetStr().GetValue()
	switch f.op {
	case modelv1.Condition_BINARY_OP_EQ:
		return strValue == f.val
	case modelv1.Condition_BINARY_OP_NE:
		return strValue != f.val
	}
	return false
}

type idTagFilter struct {
	partition.TagLocator
	op  modelv1.Condition_BinaryOp
	val string
}

func (f *idTagFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	val := tagFamilies[f.FamilyOffset].GetTags()[f.TagOffset].GetId().GetValue()
	switch f.op {
	case modelv1.Condition_BINARY_OP_EQ:
		return val == f.val
	case modelv1.Condition_BINARY_OP_NE:
		return val != f.val
	}
	return false
}

type int64TagFilter struct {
	partition.TagLocator
	op  modelv1.Condition_BinaryOp
	val int64
}

func (f *int64TagFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	val := tagFamilies[f.FamilyOffset].GetTags()[f.TagOffset].GetInt().GetValue()
	switch f.op {
	case modelv1.Condition_BINARY_OP_EQ:
		return val == f.val
	case modelv1.Condition_BINARY_OP_NE:
		return val != f.val
	case modelv1.Condition_BINARY_OP_GE:
		return val >= f.val
	case modelv1.Condition_BINARY_OP_GT:
		return val > f.val
	case modelv1.Condition_BINARY_OP_LE:
		return val <= f.val
	case modelv1.Condition_BINARY_OP_LT:
		return val < f.val
	}
	return false
}

// groupTagsLocator can be used to locate tags within families
type groupTagsLocator []partition.TagLocator

// newGroupLocator generates a groupTagsLocator which strictly preserve the order of groupByNames
func newGroupLocator(m *databasev1.Measure, groupByNames []string) (groupTagsLocator, error) {
	groupTags := make([]partition.TagLocator, 0, len(groupByNames))
	for _, groupByName := range groupByNames {
		fIdx, tIdx, spec := pbv1.FindTagByName(m.GetTagFamilies(), groupByName)
		if spec == nil {
			return nil, errors.New("tag is not found")
		}
		groupTags = append(groupTags, partition.TagLocator{
			FamilyOffset: fIdx,
			TagOffset:    tIdx,
		})
	}
	return groupTags, nil
}

func stringify(tagValue *modelv1.TagValue) string {
	switch v := tagValue.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return v.Str.GetValue()
	case *modelv1.TagValue_Id:
		return v.Id.GetValue()
	case *modelv1.TagValue_Int:
		return strconv.FormatInt(v.Int.GetValue(), 10)
	case *modelv1.TagValue_BinaryData:
		return base64.StdEncoding.EncodeToString(v.BinaryData)
	case *modelv1.TagValue_IntArray:
		return strings.Join(transform(v.IntArray.GetValue(), func(num int64) string {
			return strconv.FormatInt(num, 10)
		}), ",")
	case *modelv1.TagValue_StrArray:
		return strings.Join(v.StrArray.GetValue(), ",")
	default:
		return "<nil>"
	}
}

func transform[I, O any](input []I, mapper func(I) O) []O {
	output := make([]O, len(input))
	for i := range input {
		output[i] = mapper(input[i])
	}
	return output
}
