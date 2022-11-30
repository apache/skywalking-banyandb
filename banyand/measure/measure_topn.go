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
	"sync"
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
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	timeBucketFormat = "200601021504"
	TopNTagFamily    = "__topN__"
)

var (
	_ io.Closer = (*topNStreamingProcessor)(nil)
	_ io.Closer = (*topNProcessorManager)(nil)
	_ flow.Sink = (*topNStreamingProcessor)(nil)

	errUnsupportedConditionValueType = errors.New("unsupported value type in the condition")

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
	topNSchema       *databasev1.TopNAggregation
	sortDirection    modelv1.Sort
	databaseSupplier tsdb.Supplier
	src              chan interface{}
	in               chan flow.StreamRecord
	errCh            <-chan error
	stopCh           chan struct{}
	streamingFlow    flow.Flow
}

func (t *topNStreamingProcessor) In() chan<- flow.StreamRecord {
	return t.in
}

func (t *topNStreamingProcessor) Setup(ctx context.Context) error {
	t.Add(1)
	go t.run(ctx)
	return nil
}

func (t *topNStreamingProcessor) run(ctx context.Context) {
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
	// close streaming flow
	err := t.streamingFlow.Close()
	// and wait for error channel close
	<-t.stopCh
	t.stopCh = nil
	return err
}

func (t *topNStreamingProcessor) writeStreamRecord(record flow.StreamRecord) error {
	tuples, ok := record.Data().([]*streaming.Tuple2)
	if !ok {
		return errors.New("invalid data type")
	}
	// down-sample the start of the timeWindow to a time-bucket
	eventTime := t.downSampleTimeBucket(record.TimestampMillis())
	timeBucket := eventTime.Format(timeBucketFormat)
	var err error
	if e := t.l.Debug(); e.Enabled() {
		e.Str("TopN", t.topNSchema.GetMetadata().GetName()).
			Int("rankNums", len(tuples)).
			Msg("Write a tuple")
	}
	for rankNum, tuple := range tuples {
		fieldValue := tuple.V1.(int64)
		data := tuple.V2.(flow.StreamRecord).Data().(flow.Data)
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
	entity, entityValues, shardID, err := t.locate(tagValues, rankNum)
	if err != nil {
		return err
	}
	shard, err := t.databaseSupplier.SupplyTSDB().Shard(shardID)
	if err != nil {
		return err
	}
	series, err := shard.Series().Get(tsdb.HashEntity(entity), entityValues)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	span, err := series.Create(ctx, eventTime)
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
		builder.Family(familyIdentity(TopNTagFamily, pbv1.TagFlag), payload)
		virtualFieldValue := &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Int{
				Int: &modelv1.Int{
					Value: fieldValue,
				},
			},
		}
		fieldData := encodeFieldValue(virtualFieldValue)
		builder.Family(familyIdentity(TopNValueFieldSpec.GetName(), pbv1.EncoderFieldFlag(TopNValueFieldSpec, t.interval)), fieldData)
		writer, errWrite := builder.Build()
		if errWrite != nil {
			return nil, errWrite
		}
		_, errWrite = writer.Write()
		if e := t.l.Debug(); e.Enabled() {
			e.Time("ts", eventTime).
				Int("ts_nano", eventTime.Nanosecond()).
				Uint64("series_id", uint64(series.ID())).
				Stringer("series", series).
				Uint64("item_id", uint64(writer.ItemID().ID)).
				Int("shard_id", int(shardID)).
				Msg("write measure")
		}
		return writer, errWrite
	}
	_, err = writeFn()
	if err != nil {
		_ = span.Close()
		return err
	}
	return span.Close()
}

func (t *topNStreamingProcessor) downSampleTimeBucket(eventTimeMillis int64) time.Time {
	return time.UnixMilli(eventTimeMillis - eventTimeMillis%t.interval.Milliseconds())
}

func (t *topNStreamingProcessor) locate(tagValues []*modelv1.TagValue, rankNum int) (tsdb.Entity, tsdb.EntityValues, common.ShardID, error) {
	if len(t.topNSchema.GetGroupByTagNames()) != len(tagValues) {
		return nil, nil, 0, errors.New("no enough tag values for the entity")
	}
	// entity prefix
	// 1) source measure Name + topN aggregation Name
	// 2) sort direction
	// 3) rank number
	entity := make(tsdb.EntityValues, 1+1+1+len(t.topNSchema.GetGroupByTagNames()))
	// entity prefix
	entity[0] = tsdb.StrValue(formatMeasureCompanionPrefix(t.topNSchema.GetSourceMeasure().GetName(),
		t.topNSchema.GetMetadata().GetName()))
	entity[1] = tsdb.Int64Value(int64(t.sortDirection.Number()))
	entity[2] = tsdb.Int64Value(int64(rankNum))
	// measureID as sharding key
	for idx, tagVal := range tagValues {
		entity[idx+3] = tagVal
	}
	e, err := entity.ToEntity()
	if err != nil {
		return nil, nil, 0, err
	}
	id, err := partition.ShardID(e.Marshal(), t.shardNum)
	if err != nil {
		return nil, nil, 0, err
	}
	return e, entity, common.ShardID(id), nil
}

func (t *topNStreamingProcessor) start() *topNStreamingProcessor {
	t.errCh = t.streamingFlow.Window(streaming.NewTumblingTimeWindows(t.interval)).
		AllowedMaxWindows(int(t.topNSchema.GetLruSize())).
		TopN(int(t.topNSchema.GetCountersNumber()),
			streaming.WithSortKeyExtractor(func(record flow.StreamRecord) int64 {
				return record.Data().(flow.Data)[1].(int64)
			}),
			OrderBy(t.topNSchema.GetFieldValueSort()),
		).To(t).Open()
	go t.handleError()
	return t
}

func OrderBy(sort modelv1.Sort) streaming.TopNOption {
	if sort == modelv1.Sort_SORT_ASC {
		return streaming.OrderBy(streaming.ASC)
	}
	return streaming.OrderBy(streaming.DESC)
}

func (t *topNStreamingProcessor) handleError() {
	for err := range t.errCh {
		t.l.Err(err).Str("topN", t.topNSchema.GetMetadata().GetName()).
			Msg("error occurred during flow setup or process")
	}
	t.stopCh <- struct{}{}
}

// topNProcessorManager manages multiple topNStreamingProcessor(s) belonging to a single measure
type topNProcessorManager struct {
	// RWMutex here is to protect the processorMap from data race, i.e.
	// the send operation to the underlying channel vs. the close of the channel
	// TODO: this can be optimized if the bus Listener can be synchronously finished,
	sync.RWMutex
	l            *logger.Logger
	m            *measure
	topNSchemas  []*databasev1.TopNAggregation
	processorMap map[*commonv1.Metadata][]*topNStreamingProcessor
}

func (manager *topNProcessorManager) Close() error {
	manager.Lock()
	defer manager.Unlock()
	var err error
	for _, processorList := range manager.processorMap {
		for _, processor := range processorList {
			err = multierr.Append(err, processor.Close())
		}
	}
	return err
}

func (manager *topNProcessorManager) onMeasureWrite(request *measurev1.WriteRequest) {
	go func() {
		manager.RLock()
		defer manager.RUnlock()
		for _, processorList := range manager.processorMap {
			for _, processor := range processorList {
				processor.src <- flow.NewStreamRecordWithTimestampPb(request.GetDataPoint(), request.GetDataPoint().GetTimestamp())
			}
		}
	}()
}

func (manager *topNProcessorManager) start() error {
	interval := manager.m.interval
	for _, topNSchema := range manager.topNSchemas {
		sortDirections := make([]modelv1.Sort, 0, 2)
		if topNSchema.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
			sortDirections = append(sortDirections, modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_DESC)
		} else {
			sortDirections = append(sortDirections, topNSchema.GetFieldValueSort())
		}

		processorList := make([]*topNStreamingProcessor, len(sortDirections))
		for i, sortDirection := range sortDirections {
			srcCh := make(chan interface{})
			src, _ := sources.NewChannel(srcCh)
			streamingFlow := streaming.New(src)

			filters, buildErr := manager.buildFilter(topNSchema.GetCriteria())
			if buildErr != nil {
				return buildErr
			}
			streamingFlow = streamingFlow.Filter(filters)

			mapper, innerErr := manager.buildMapper(topNSchema.GetFieldName(), topNSchema.GetGroupByTagNames()...)
			if innerErr != nil {
				return innerErr
			}
			streamingFlow = streamingFlow.Map(mapper)

			processor := &topNStreamingProcessor{
				l:                manager.l,
				shardNum:         manager.m.shardNum,
				interval:         interval,
				topNSchema:       topNSchema,
				sortDirection:    sortDirection,
				databaseSupplier: manager.m.databaseSupplier,
				src:              srcCh,
				in:               make(chan flow.StreamRecord),
				stopCh:           make(chan struct{}),
				streamingFlow:    streamingFlow,
			}
			processorList[i] = processor.start()
		}

		manager.processorMap[topNSchema.GetSourceMeasure()] = processorList
	}

	return nil
}

func (manager *topNProcessorManager) buildFilter(criteria *modelv1.Criteria) (flow.UnaryFunc[bool], error) {
	// if criteria is nil, we handle all incoming elements
	if criteria == nil {
		return func(_ context.Context, dataPoint any) bool {
			return true
		}, nil
	}

	f, err := manager.buildFilterForCriteria(criteria)
	if err != nil {
		return nil, err
	}

	return func(_ context.Context, dataPoint any) bool {
		tfs := dataPoint.(*measurev1.DataPointValue).GetTagFamilies()
		return f.predicate(tfs)
	}, nil
}

func (manager *topNProcessorManager) buildFilterForCriteria(criteria *modelv1.Criteria) (conditionFilter, error) {
	switch v := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		return manager.buildFilterForCondition(v.Condition)
	case *modelv1.Criteria_Le:
		return manager.buildFilterForLogicalExpr(v.Le)
	default:
		return nil, errors.New("should not reach here")
	}
}

// buildFilterForCondition builds a logical and composable filter for a logical expression which have underlying conditions,
// or nested logical expressions as its children
func (manager *topNProcessorManager) buildFilterForLogicalExpr(logicalExpr *modelv1.LogicalExpression) (conditionFilter, error) {
	left, lErr := manager.buildFilterForCriteria(logicalExpr.Left)
	if lErr != nil {
		return nil, lErr
	}
	right, rErr := manager.buildFilterForCriteria(logicalExpr.Right)
	if rErr != nil {
		return nil, rErr
	}
	return composeWithOp(left, right, logicalExpr.Op), nil
}

func composeWithOp(left, right conditionFilter, op modelv1.LogicalExpression_LogicalOp) conditionFilter {
	if op == modelv1.LogicalExpression_LOGICAL_OP_AND {
		return &andFilter{left, right}
	}
	return &orFilter{left, right}
}

// buildFilterForCondition builds a single, composable filter for a single condition
func (manager *topNProcessorManager) buildFilterForCondition(cond *modelv1.Condition) (conditionFilter, error) {
	familyOffset, tagOffset, spec := pbv1.FindTagByName(manager.m.GetSchema().GetTagFamilies(), cond.GetName())
	if spec == nil {
		return nil, errors.New("fail to parse tag by name")
	}
	switch v := cond.GetValue().GetValue().(type) {
	case *modelv1.TagValue_Int:
		return &int64TagFilter{
			TagLocator: partition.TagLocator{
				FamilyOffset: familyOffset,
				TagOffset:    tagOffset,
			},
			op:  cond.GetOp(),
			val: v.Int.GetValue(),
		}, nil
	case *modelv1.TagValue_Str:
		return &strTagFilter{
			TagLocator: partition.TagLocator{
				FamilyOffset: familyOffset,
				TagOffset:    tagOffset,
			},
			op:  cond.GetOp(),
			val: v.Str.GetValue(),
		}, nil
	case *modelv1.TagValue_Id:
		return &idTagFilter{
			TagLocator: partition.TagLocator{
				FamilyOffset: familyOffset,
				TagOffset:    tagOffset,
			},
			op:  cond.GetOp(),
			val: v.Id.GetValue(),
		}, nil
	default:
		return nil, errUnsupportedConditionValueType
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

type andFilter struct {
	l, r conditionFilter
}

func (f *andFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	return f.l.predicate(tagFamilies) && f.r.predicate(tagFamilies)
}

type orFilter struct {
	l, r conditionFilter
}

func (f *orFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	return f.l.predicate(tagFamilies) || f.r.predicate(tagFamilies)
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
