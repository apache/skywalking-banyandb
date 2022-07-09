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
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	_ bus.MessageListener = (*topNProcessCallback)(nil)
	_ io.Closer           = (*topNProcessor)(nil)
	_ io.Closer           = (*topNProcessorManager)(nil)
	_ streamingApi.Sink   = (*topNProcessor)(nil)
)

type (
	measureProcessorFilter func(request *measurev1.DataPointValue) bool
	measureProcessorMapper func(request *measurev1.DataPointValue) api.Data
)

type topNProcessor struct {
	l                *logger.Logger
	shardNum         uint32
	topNSchema       *databasev1.TopNAggregation
	databaseSupplier tsdb.Supplier
	src              chan interface{}
	in               chan interface{}
	errCh            <-chan error
	streamingFlow    api.Flow
}

func (t *topNProcessor) In() chan<- interface{} {
	return t.in
}

func (t *topNProcessor) Setup(ctx context.Context) error {
	go t.run(ctx)
	return nil
}

func (t *topNProcessor) run(ctx context.Context) {
	for {
		select {
		case item, open := <-t.in:
			if !open {
				return
			}
			if record, ok := item.(api.StreamRecord); ok {
				if err := t.write(record); err != nil {
					t.l.Err(err).Msg("fail to write stream record")
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (t *topNProcessor) Teardown(ctx context.Context) error {
	return nil
}

func (t *topNProcessor) Close() error {
	close(t.src)
	return nil
}

func (t *topNProcessor) write(record api.StreamRecord) error {
	entity, shardID, err := t.locate()
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
	startTs := time.UnixMilli(record.TimestampMillis())
	span, err := series.Span(timestamp.NewInclusiveTimeRangeDuration(startTs, 0))
	if err != nil {
		if span != nil {
			_ = span.Close()
		}
		return err
	}
	writeFn := func() (tsdb.Writer, error) {
		builder := span.WriterBuilder().Time(startTs)
		virtualFamily := &modelv1.TagFamilyForWrite{
			Tags: []*modelv1.TagValue{
				{
					Value: &modelv1.TagValue_BinaryData{
						BinaryData: nil, // TODO: how to serialize result set
					},
				},
			},
		}
		payload, errMarshal := proto.Marshal(virtualFamily)
		if errMarshal != nil {
			return nil, errMarshal
		}
		builder.Family(familyIdentity("__topN__", TagFlag), payload)
		writer, errWrite := builder.Build()
		if errWrite != nil {
			return nil, errWrite
		}
		_, errWrite = writer.Write()
		t.l.Debug().
			Time("ts", startTs).
			Int("ts_nano", startTs.Nanosecond()).
			Interface("data", record).
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

func (t *topNProcessor) locate() (tsdb.Entity, common.ShardID, error) {
	entity := make(tsdb.Entity, 1)
	entity[0] = []byte(formatMeasureCompanionPrefix(t.topNSchema.GetSourceMeasure().GetName(),
		t.topNSchema.GetMetadata().GetName()))
	id, err := partition.ShardID(entity.Marshal(), t.shardNum)
	if err != nil {
		return nil, 0, err
	}
	return entity, common.ShardID(id), nil
}

// topNProcessorManager manages multiple topNProcessor(s) belonging to a single measure
type topNProcessorManager struct {
	l            *logger.Logger
	m            *measure
	topNSchemas  []*databasev1.TopNAggregation
	processorMap map[*commonv1.Metadata]*topNProcessor
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
		processor.src <- api.NewStreamRecordWithTimestampPb(request.GetDataPoint(), request.GetDataPoint().GetTimestamp())
	}

	return nil
}

func (manager *topNProcessorManager) start() error {
	for _, topNSchema := range manager.topNSchemas {
		srcCh := make(chan interface{})
		streamingFlow := streaming.New(srcCh)

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

		processor := &topNProcessor{
			l:                manager.l,
			shardNum:         manager.m.shardNum,
			topNSchema:       topNSchema,
			databaseSupplier: manager.m.databaseSupplier,
			src:              srcCh,
			in:               make(chan interface{}),
			streamingFlow:    streamingFlow,
		}

		// TODO: how to set size and slide
		processor.errCh = streamingFlow.Window(streaming.NewSlidingTimeWindows(60*time.Second, 25*time.Second)).
			TopN(int(topNSchema.GetCountersNumber()),
				streaming.WithSortKeyExtractor(func(elem interface{}) int64 {
					return elem.(api.Data)[1].(int64)
				}),
							streaming.WithCacheSize(int(topNSchema.GetCountersNumber()))).
			To(processor).OpenAsync() // TODO: add destination impl

		manager.processorMap[topNSchema.GetSourceMeasure()] = processor
	}

	return nil
}

func (manager *topNProcessorManager) buildFilters(criteria []*modelv1.Criteria) (measureProcessorFilter, error) {
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
		return func(value *measurev1.DataPointValue) bool {
			return true
		}, nil
	}

	return func(dataPoint *measurev1.DataPointValue) bool {
		tfs := dataPoint.GetTagFamilies()
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

func (manager *topNProcessorManager) buildMapper(fieldName string, groupByNames ...string) (measureProcessorMapper, error) {
	fieldIdx := slices.IndexFunc(manager.m.GetSchema().GetFields(), func(spec *databasev1.FieldSpec) bool {
		return spec.GetName() == fieldName
	})
	if fieldIdx == -1 {
		return nil, errors.New("invalid fieldName")
	}
	groupLocator, err := newGroupLocator(manager.m.GetSchema(), groupByNames)
	if err != nil {
		return nil, err
	}
	return func(request *measurev1.DataPointValue) api.Data {
		return api.Row(
			strings.Join(transform(groupLocator, func(locator partition.TagLocator) string {
				return stringify(request.GetTagFamilies()[locator.FamilyOffset].GetTags()[locator.TagOffset])
			}), "|"),
			// TODO: we only support int64
			request.GetFields()[fieldIdx].GetInt().GetValue(),
		)
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

type groupTagsLocator []partition.TagLocator

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
