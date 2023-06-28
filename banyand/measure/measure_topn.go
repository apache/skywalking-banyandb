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
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	apiData "github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const (
	timeBucketFormat = "200601021504"
	// TopNTagFamily is the identity of a tag family which contains the topN calculated result.
	TopNTagFamily = "__topN__"
)

var (
	_ io.Closer = (*topNStreamingProcessor)(nil)
	_ io.Closer = (*topNProcessorManager)(nil)
	_ flow.Sink = (*topNStreamingProcessor)(nil)

	// TopNValueFieldSpec denotes the field specification of the topN calculated result.
	TopNValueFieldSpec = &databasev1.FieldSpec{
		Name:              "value",
		FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}
)

type dataPointWithEntityValues struct {
	*measurev1.DataPointValue
	entityValues tsdb.EntityValues
}

type topNStreamingProcessor struct {
	m             *measure
	streamingFlow flow.Flow
	l             *logger.Logger
	pipeline      queue.Queue
	topNSchema    *databasev1.TopNAggregation
	src           chan interface{}
	in            chan flow.StreamRecord
	errCh         <-chan error
	stopCh        chan struct{}
	flow.ComponentState
	interval      time.Duration
	sortDirection modelv1.Sort
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
			// nolint: contextcheck
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
func (t *topNStreamingProcessor) Teardown(_ context.Context) error {
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
	tuplesGroups, ok := record.Data().(map[string][]*streaming.Tuple2)
	if !ok {
		return errors.New("invalid data type")
	}
	// down-sample the start of the timeWindow to a time-bucket
	eventTime := t.downSampleTimeBucket(record.TimestampMillis())
	timeBucket := eventTime.Format(timeBucketFormat)
	var err error
	for group, tuples := range tuplesGroups {
		if e := t.l.Debug(); e.Enabled() {
			e.Str("TopN", t.topNSchema.GetMetadata().GetName()).
				Str("group", group).
				Int("rankNums", len(tuples)).
				Msg("Write tuples")
		}
		for rankNum, tuple := range tuples {
			fieldValue := tuple.V1.(int64)
			data := tuple.V2.(flow.StreamRecord).Data().(flow.Data)
			err = multierr.Append(err, t.writeData(eventTime, timeBucket, fieldValue, group, data, rankNum))
		}
	}
	return err
}

func (t *topNStreamingProcessor) writeData(eventTime time.Time, timeBucket string, fieldValue int64,
	group string, data flow.Data, rankNum int,
) error {
	var tagValues []*modelv1.TagValue
	if len(t.topNSchema.GetGroupByTagNames()) > 0 {
		var ok bool
		if tagValues, ok = data[3].([]*modelv1.TagValue); !ok {
			return errors.New("fail to extract tag values from topN result")
		}
	}
	entity, entityValues, shardID, err := t.locate(tagValues, rankNum)
	if err != nil {
		return err
	}

	// measureID is consist of three parts,
	// 1. groupValues
	// 2. rankNumber
	// 3. timeBucket
	measureID := group + "_" + strconv.Itoa(rankNum) + "_" + timeBucket
	iwr := &measurev1.InternalWriteRequest{
		Request: &measurev1.WriteRequest{
			Metadata: t.topNSchema.GetMetadata(),
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(eventTime),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{
						Tags: append([]*modelv1.TagValue{
							// MeasureID
							{
								Value: &modelv1.TagValue_Str{
									Str: &modelv1.Str{
										Value: measureID,
									},
								},
							},
						}, data[0].(tsdb.EntityValues)...),
					},
				},
				Fields: []*modelv1.FieldValue{
					{
						Value: &modelv1.FieldValue_Int{
							Int: &modelv1.Int{
								Value: fieldValue,
							},
						},
					},
				},
			},
		},
		ShardId:    uint32(shardID),
		SeriesHash: tsdb.HashEntity(entity),
	}
	if t.l.Debug().Enabled() {
		iwr.EntityValues = entityValues.Encode()
	}
	message := bus.NewMessage(bus.MessageID(time.Now().UnixNano()), iwr)
	_, errWritePub := t.pipeline.Publish(apiData.TopicMeasureWrite, message)
	return errWritePub
}

func (t *topNStreamingProcessor) downSampleTimeBucket(eventTimeMillis int64) time.Time {
	return time.UnixMilli(eventTimeMillis - eventTimeMillis%t.interval.Milliseconds())
}

func (t *topNStreamingProcessor) locate(tagValues []*modelv1.TagValue, rankNum int) (tsdb.Entity, tsdb.EntityValues, common.ShardID, error) {
	if len(tagValues) != 0 && len(t.topNSchema.GetGroupByTagNames()) != len(tagValues) {
		return nil, nil, 0, errors.New("no enough tag values for the entity")
	}
	// entity prefix
	// 1) source measure Name + topN aggregation Name
	// 2) sort direction
	// 3) rank number
	// >4) group tag values if needed
	entity := make(tsdb.EntityValues, 1+1+1+len(tagValues))
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
	id, err := partition.ShardID(e.Marshal(), t.m.shardNum)
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
				return record.Data().(flow.Data)[2].(int64)
			}),
			orderBy(t.topNSchema.GetFieldValueSort()),
			streaming.WithGroupKeyExtractor(func(record flow.StreamRecord) string {
				return record.Data().(flow.Data)[1].(string)
			}),
		).To(t).Open()
	go t.handleError()
	return t
}

func orderBy(sort modelv1.Sort) streaming.TopNOption {
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

// topNProcessorManager manages multiple topNStreamingProcessor(s) belonging to a single measure.
type topNProcessorManager struct {
	l            *logger.Logger
	pipeline     queue.Queue
	m            *measure
	s            logical.TagSpecRegistry
	processorMap map[*commonv1.Metadata][]*topNStreamingProcessor
	topNSchemas  []*databasev1.TopNAggregation
	sync.RWMutex
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

func (manager *topNProcessorManager) onMeasureWrite(request *measurev1.InternalWriteRequest) {
	go func() {
		manager.RLock()
		defer manager.RUnlock()
		for _, processorList := range manager.processorMap {
			for _, processor := range processorList {
				processor.src <- flow.NewStreamRecordWithTimestampPb(&dataPointWithEntityValues{
					request.GetRequest().GetDataPoint(),
					request.GetEntityValues(),
				}, request.GetRequest().GetDataPoint().GetTimestamp())
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
				m:             manager.m,
				l:             manager.l,
				interval:      interval,
				topNSchema:    topNSchema,
				sortDirection: sortDirection,
				src:           srcCh,
				in:            make(chan flow.StreamRecord),
				stopCh:        make(chan struct{}),
				streamingFlow: streamingFlow,
				pipeline:      manager.pipeline,
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

	f, err := logical.BuildSimpleTagFilter(criteria)
	if err != nil {
		return nil, err
	}

	return func(_ context.Context, request any) bool {
		tffws := request.(*dataPointWithEntityValues).GetTagFamilies()
		ok, matchErr := f.Match(logical.TagFamiliesForWrite(tffws), manager.s)
		if matchErr != nil {
			manager.l.Err(matchErr).Msg("fail to match criteria")
			return false
		}
		return ok
	}, nil
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
			dpWithEvs := request.(*dataPointWithEntityValues)
			if len(dpWithEvs.GetFields()) <= fieldIdx {
				manager.l.Warn().Interface("point", dpWithEvs.DataPointValue).
					Str("fieldName", fieldName).
					Int("len", len(dpWithEvs.GetFields())).
					Int("fieldIdx", fieldIdx).
					Msg("out of range")
			}
			return flow.Data{
				// EntityValues as identity
				dpWithEvs.entityValues,
				// save string representation of group values as the key, i.e. v1
				"",
				// field value as v2
				// TODO: we only support int64
				dpWithEvs.GetFields()[fieldIdx].GetInt().GetValue(),
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
		dpWithEvs := request.(*dataPointWithEntityValues)
		return flow.Data{
			// EntityValues as identity
			dpWithEvs.entityValues,
			// save string representation of group values as the key, i.e. v1
			strings.Join(transform(groupLocator, func(locator partition.TagLocator) string {
				return stringify(extractTagValue(dpWithEvs.DataPointValue, locator))
			}), "|"),
			// field value as v2
			// TODO: we only support int64
			dpWithEvs.GetFields()[fieldIdx].GetInt().GetValue(),
			// groupBy tag values as v3
			transform(groupLocator, func(locator partition.TagLocator) *modelv1.TagValue {
				return extractTagValue(dpWithEvs.DataPointValue, locator)
			}),
		}
	}, nil
}

// groupTagsLocator can be used to locate tags within families.
type groupTagsLocator []partition.TagLocator

// newGroupLocator generates a groupTagsLocator which strictly preserve the order of groupByNames.
func newGroupLocator(m *databasev1.Measure, groupByNames []string) (groupTagsLocator, error) {
	groupTags := make([]partition.TagLocator, 0, len(groupByNames))
	for _, groupByName := range groupByNames {
		fIdx, tIdx, spec := pbv1.FindTagByName(m.GetTagFamilies(), groupByName)
		if spec == nil {
			return nil, fmt.Errorf("tag %s is not found", groupByName)
		}
		groupTags = append(groupTags, partition.TagLocator{
			FamilyOffset: fIdx,
			TagOffset:    tIdx,
		})
	}
	return groupTags, nil
}

func extractTagValue(dpv *measurev1.DataPointValue, locator partition.TagLocator) *modelv1.TagValue {
	if locator.FamilyOffset >= len(dpv.GetTagFamilies()) {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	tagFamily := dpv.GetTagFamilies()[locator.FamilyOffset]
	if locator.TagOffset >= len(tagFamily.GetTags()) {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	return tagFamily.GetTags()[locator.TagOffset]
}

func stringify(tagValue *modelv1.TagValue) string {
	switch v := tagValue.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return v.Str.GetValue()
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
		return ""
	}
}

func transform[I, O any](input []I, mapper func(I) O) []O {
	output := make([]O, len(input))
	for i := range input {
		output[i] = mapper(input[i])
	}
	return output
}
