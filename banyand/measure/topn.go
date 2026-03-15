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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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

	apiData "github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	timeBucketFormat         = "200601021504"
	resultPersistencyTimeout = 10 * time.Second
	maxFlushInterval         = time.Minute
)

var (
	_ io.Closer = (*topNStreamingProcessor[int64])(nil)
	_ io.Closer = (*topNProcessorManager)(nil)
	_ flow.Sink = (*topNStreamingProcessor[int64])(nil)
	_ io.Closer = (*topNStreamingProcessor[float64])(nil)
	_ flow.Sink = (*topNStreamingProcessor[float64])(nil)
)

func (sr *schemaRepo) inFlow(
	stm *databasev1.Measure,
	seriesID uint64,
	shardID uint32,
	entityValues []*modelv1.TagValue,
	dp *measurev1.DataPointValue,
	spec *measurev1.DataPointSpec,
) {
	if p, _ := sr.topNProcessorMap.Load(getKey(stm.GetMetadata())); p != nil {
		p.(*topNProcessorManager).onMeasureWrite(seriesID, shardID, &measurev1.InternalWriteRequest{
			Request: &measurev1.WriteRequest{
				Metadata:      stm.GetMetadata(),
				DataPoint:     dp,
				MessageId:     uint64(time.Now().UnixNano()),
				DataPointSpec: spec,
			},
			EntityValues: entityValues,
		}, stm)
	}
}

func (sr *schemaRepo) getSteamingManager(source *commonv1.Metadata, pipeline queue.Client) (manager *topNProcessorManager) {
	key := getKey(source)
	// avoid creating a new manager if the source group is closing
	if sr.isGroupClosing(source.GetGroup()) {
		return nil
	}
	sourceMeasure, ok := sr.loadMeasure(source)
	if !ok {
		m, _ := sr.topNProcessorMap.LoadOrStore(key, &topNProcessorManager{
			l:        sr.l,
			pipeline: pipeline,
			nodeID:   sr.nodeID,
		})
		manager = m.(*topNProcessorManager)
		return manager
	}

	if v, ok := sr.topNProcessorMap.Load(key); ok {
		pre := v.(*topNProcessorManager)
		pre.init(sourceMeasure.GetSchema())
		var modRevision int64
		pre.RLock()
		modRevision = pre.m.GetMetadata().GetModRevision()
		pre.RUnlock()
		if modRevision < sourceMeasure.schema.GetMetadata().GetModRevision() {
			defer pre.Close()
			manager = &topNProcessorManager{
				l:        sr.l,
				pipeline: pipeline,
				nodeID:   sr.nodeID,
			}
			manager.registeredTasks = append(manager.registeredTasks, pre.registeredTasks...)
		} else {
			return pre
		}
	}
	if manager == nil {
		manager = &topNProcessorManager{
			l:        sr.l,
			pipeline: pipeline,
			nodeID:   sr.nodeID,
		}
	}
	manager.init(sourceMeasure.GetSchema())
	sr.topNProcessorMap.Store(key, manager)
	return manager
}

func (sr *schemaRepo) stopSteamingManager(sourceMeasure *commonv1.Metadata) {
	key := getKey(sourceMeasure)
	if v, ok := sr.topNProcessorMap.Load(key); ok {
		v.(*topNProcessorManager).Close()
		sr.topNProcessorMap.Delete(key)
	}
}

type dataPointWithEntityValues struct {
	tagSpec logical.TagSpecRegistry
	*measurev1.DataPointValue
	fieldIndex   map[string]int
	entityValues []*modelv1.TagValue
	seriesID     uint64
	shardID      uint32
}

func newDataPointWithEntityValues(
	dp *measurev1.DataPointValue,
	entityValues []*modelv1.TagValue,
	seriesID uint64,
	shardID uint32,
	spec *measurev1.DataPointSpec,
	m *databasev1.Measure,
) *dataPointWithEntityValues {
	fieldIndex := buildFieldIndex(spec, m)
	tagSpec := buildTagSpecRegistryFromSpec(spec, m)
	return &dataPointWithEntityValues{
		DataPointValue: dp,
		entityValues:   entityValues,
		seriesID:       seriesID,
		shardID:        shardID,
		tagSpec:        tagSpec,
		fieldIndex:     fieldIndex,
	}
}

func buildFieldIndex(spec *measurev1.DataPointSpec, m *databasev1.Measure) map[string]int {
	if spec != nil {
		fieldIndex := make(map[string]int, len(spec.GetFieldNames()))
		for i, fieldName := range spec.GetFieldNames() {
			fieldIndex[fieldName] = i
		}
		return fieldIndex
	}
	if m != nil {
		fieldIndex := make(map[string]int, len(m.GetFields()))
		for i, fieldSpec := range m.GetFields() {
			fieldIndex[fieldSpec.GetName()] = i
		}
		return fieldIndex
	}
	return make(map[string]int)
}

func buildTagSpecRegistryFromSpec(spec *measurev1.DataPointSpec, m *databasev1.Measure) logical.TagSpecRegistry {
	tagSpecMap := logical.TagSpecMap{}
	if spec != nil {
		for specFamilyIdx, specFamily := range spec.GetTagFamilySpec() {
			for specTagIdx, tagName := range specFamily.GetTagNames() {
				tagSpec := &databasev1.TagSpec{
					Name: tagName,
				}
				tagSpecMap.RegisterTag(specFamilyIdx, specTagIdx, tagSpec)
			}
		}
		return tagSpecMap
	}
	if m != nil {
		for specFamilyIdx, tagFamily := range m.GetTagFamilies() {
			for specTagIdx, tagSpec := range tagFamily.GetTags() {
				tagSpecMap.RegisterTag(specFamilyIdx, specTagIdx, tagSpec)
			}
		}
		return tagSpecMap
	}
	return tagSpecMap
}

func (dp *dataPointWithEntityValues) tagValue(tagName string) *modelv1.TagValue {
	if familyIdx, tagIdx, ok := dp.locateSpecTag(tagName); ok {
		if familyIdx < len(dp.GetTagFamilies()) {
			tagFamily := dp.GetTagFamilies()[familyIdx]
			if tagIdx < len(tagFamily.GetTags()) {
				return tagFamily.GetTags()[tagIdx]
			}
		}
		return pbv1.NullTagValue
	}
	return pbv1.NullTagValue
}

func (dp *dataPointWithEntityValues) locateSpecTag(tagName string) (int, int, bool) {
	if dp.tagSpec == nil {
		return 0, 0, false
	}
	tagSpecFound := dp.tagSpec.FindTagSpecByName(tagName)
	if tagSpecFound == nil {
		return 0, 0, false
	}
	familyIdx := tagSpecFound.TagFamilyIdx
	tagIdx := tagSpecFound.TagIdx
	if familyIdx < 0 || tagIdx < 0 {
		return 0, 0, false
	}
	return familyIdx, tagIdx, true
}

func (dp *dataPointWithEntityValues) intFieldValue(fieldName string, l *logger.Logger) int64 {
	if dp.fieldIndex == nil {
		return 0
	}
	fieldIdx, ok := dp.fieldIndex[fieldName]
	if !ok {
		return 0
	}
	if fieldIdx >= len(dp.GetFields()) {
		if l != nil {
			l.Warn().Str("fieldName", fieldName).
				Int("len", len(dp.GetFields())).
				Int("fieldIdx", fieldIdx).
				Msg("field index out of range")
		}
		return 0
	}
	field := dp.GetFields()[fieldIdx]
	if field.GetInt() == nil {
		return 0
	}
	return field.GetInt().GetValue()
}

func (dp *dataPointWithEntityValues) floatFieldValue(fieldName string, l *logger.Logger) float64 {
	if dp.fieldIndex == nil {
		return 0
	}
	fieldIdx, ok := dp.fieldIndex[fieldName]
	if !ok {
		return 0
	}
	if fieldIdx >= len(dp.GetFields()) {
		if l != nil {
			l.Warn().Str("fieldName", fieldName).
				Int("len", len(dp.GetFields())).
				Int("fieldIdx", fieldIdx).
				Msg("field index out of range")
		}
		return 0
	}
	field := dp.GetFields()[fieldIdx]
	if field.GetFloat() == nil {
		return 0
	}
	return field.GetFloat().GetValue()
}

func (dp *dataPointWithEntityValues) fieldValue(fieldName string, fieldType databasev1.FieldType, l *logger.Logger) interface{} {
	switch fieldType {
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return dp.floatFieldValue(fieldName, l)
	default:
		return dp.intFieldValue(fieldName, l)
	}
}

type topNProcessor interface {
	In() chan<- flow.StreamRecord
	Setup(ctx context.Context) error
	Teardown(ctx context.Context) error
	Close() error
	Src() chan interface{}
	TopNSchema() *databasev1.TopNAggregation
}

type topNStreamingProcessor[N aggregation.Number] struct {
	pipeline      queue.Client
	streamingFlow flow.Flow
	in            chan flow.StreamRecord
	l             *logger.Logger
	topNSchema    *databasev1.TopNAggregation
	src           chan interface{}
	m             *databasev1.Measure
	errCh         <-chan error
	stopCh        chan struct{}
	nodeID        string
	flow.ComponentState
	interval      time.Duration
	sortDirection modelv1.Sort
	fieldType     databasev1.FieldType
}

func (t *topNStreamingProcessor[N]) In() chan<- flow.StreamRecord {
	return t.in
}

func (t *topNStreamingProcessor[N]) Setup(ctx context.Context) error {
	t.Add(1)
	go t.run(ctx)
	return nil
}

func (t *topNStreamingProcessor[N]) run(ctx context.Context) {
	defer t.Done()
	buf := make([]byte, 0, 64)
	for {
		select {
		case record, ok := <-t.in:
			if !ok {
				return
			}
			// nolint: contextcheck
			if err := t.writeStreamRecord(record, buf); err != nil {
				t.l.Err(err).Msg("fail to write stream record")
			}
		case <-ctx.Done():
			return
		}
	}
}

// Teardown is called by the Flow as a lifecycle hook.
// So we should not block on err channel within this method.
func (t *topNStreamingProcessor[N]) Teardown(_ context.Context) error {
	t.Wait()
	return nil
}

func (t *topNStreamingProcessor[N]) Close() error {
	close(t.src)
	// close streaming flow
	err := t.streamingFlow.Close()
	// and wait for error channel close
	<-t.stopCh
	t.stopCh = nil
	return err
}

func (t *topNStreamingProcessor[N]) Src() chan interface{} {
	return t.src
}

func (t *topNStreamingProcessor[N]) TopNSchema() *databasev1.TopNAggregation {
	return t.topNSchema
}

func (t *topNStreamingProcessor[N]) writeStreamRecord(record flow.StreamRecord, buf []byte) error {
	tuplesGroups, ok := record.Data().(map[string][]*streaming.Tuple2)
	if !ok {
		return errors.New("invalid data type")
	}
	// down-sample the start of the timeWindow to a time-bucket
	eventTime := t.downSampleTimeBucket(record.TimestampMillis())
	publisher := t.pipeline.NewBatchPublisher(resultPersistencyTimeout)
	defer publisher.Close()

	topNValue := generateTopNValue[N]()
	defer releaseTopNValue(topNValue)
	var err error
	for group, tuples := range tuplesGroups {
		if e := t.l.Debug(); e.Enabled() {
			for i := range tuples {
				tuple := tuples[i]
				data := tuple.V2.(flow.StreamRecord).Data().(flow.Data)
				e.
					Int("rankNums", i+1).
					Str("entityValues", fmt.Sprintf("%v", data[0])).
					Interface("value", data[2]).
					Time("eventTime", eventTime).
					Msgf("Write tuples %s %s", t.topNSchema.GetMetadata().GetName(), group)
			}
		}
		topNValue.Reset()
		topNValue.setMetadata(t.topNSchema.GetFieldName(), t.m.Entity.TagNames)
		var shardID uint32
		for _, tuple := range tuples {
			data := tuple.V2.(flow.StreamRecord).Data().(flow.Data)
			topNValue.addValue(tuple.V1.(N), data[0].([]*modelv1.TagValue))
			shardID = data[3].(uint32)
		}

		params := &TopNParameters{
			Limit: int64(t.topNSchema.CountersNumber),
		}
		paramsStr := params.String()

		entityValues := []*modelv1.TagValue{
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: t.topNSchema.GetMetadata().GetName(),
					},
				},
			},
			{
				Value: &modelv1.TagValue_Int{
					Int: &modelv1.Int{
						Value: int64(t.sortDirection),
					},
				},
			},
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: group,
					},
				},
			},
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: paramsStr,
					},
				},
			},
		}
		buf = buf[:0]
		if buf, err = topNValue.marshal(buf); err != nil {
			return err
		}
		iwr := &measurev1.InternalWriteRequest{
			Request: &measurev1.WriteRequest{
				MessageId: uint64(time.Now().UnixNano()),
				Metadata:  &commonv1.Metadata{Name: TopNSchemaName, Group: t.topNSchema.GetMetadata().Group},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(eventTime),
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{Tags: entityValues},
					},
					Fields: []*modelv1.FieldValue{
						{
							Value: &modelv1.FieldValue_BinaryData{
								BinaryData: bytes.Clone(buf),
							},
						},
					},
					Version: time.Now().UnixNano(),
				},
			},
			EntityValues: entityValues,
			ShardId:      shardID,
		}
		message := bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), "local", iwr)
		_, err = publisher.Publish(context.TODO(), apiData.TopicMeasureWrite, message)
		if err != nil {
			return err
		}
	}
	return err
}

func (t *topNStreamingProcessor[N]) downSampleTimeBucket(eventTimeMillis int64) time.Time {
	return time.UnixMilli(eventTimeMillis - eventTimeMillis%t.interval.Milliseconds())
}

func (t *topNStreamingProcessor[N]) start() *topNStreamingProcessor[N] {
	flushInterval := t.interval
	if flushInterval > maxFlushInterval {
		flushInterval = maxFlushInterval
	}
	t.errCh = t.streamingFlow.Window(streaming.NewTumblingTimeWindows(t.interval, flushInterval)).
		AllowedMaxWindows(int(t.topNSchema.GetLruSize())).
		TopN(int(t.topNSchema.GetCountersNumber()),
			streaming.WithKeyExtractor(func(record flow.StreamRecord) uint64 {
				return record.Data().(flow.Data)[4].(uint64)
			}),
			streaming.WithSortKeyExtractor(func(record flow.StreamRecord) interface{} {
				return record.Data().(flow.Data)[2]
			}),
			orderBy(t.topNSchema.GetFieldValueSort()),
			streaming.WithGroupKeyExtractor(func(record flow.StreamRecord) string {
				return record.Data().(flow.Data)[1].(string)
			}),
			streaming.WithFieldType(t.fieldType),
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

func (t *topNStreamingProcessor[N]) handleError() {
	for err := range t.errCh {
		t.l.Err(err).Str("topN", t.topNSchema.GetMetadata().GetName()).
			Msg("error occurred during flow setup or process")
	}
	// signal Close() that flow exited
	close(t.stopCh)
}

// topNProcessorManager manages multiple topNStreamingProcessor(s) belonging to a single measure.
type topNProcessorManager struct {
	pipeline        queue.Client
	l               *logger.Logger
	m               *databasev1.Measure
	nodeID          string
	registeredTasks []*databasev1.TopNAggregation
	processorList   []topNProcessor
	sync.RWMutex
	closed bool
}

func (manager *topNProcessorManager) init(m *databasev1.Measure) {
	manager.Lock()
	defer manager.Unlock()
	if manager.closed {
		return
	}
	if manager.m != nil {
		return
	}
	manager.m = m
	for i := range manager.registeredTasks {
		if err := manager.start(manager.registeredTasks[i]); err != nil {
			manager.l.Err(err).Msg("fail to start processor")
		}
	}
}

func (manager *topNProcessorManager) Close() error {
	manager.Lock()
	defer manager.Unlock()
	if manager.closed {
		return nil
	}
	manager.closed = true
	var err error
	for _, processor := range manager.processorList {
		err = multierr.Append(err, processor.Close())
	}
	manager.processorList = nil
	manager.registeredTasks = nil
	manager.m = nil
	return err
}

func (manager *topNProcessorManager) onMeasureWrite(seriesID uint64, shardID uint32, request *measurev1.InternalWriteRequest, measure *databasev1.Measure) {
	go func() {
		manager.RLock()
		defer manager.RUnlock()
		if manager.closed {
			return
		}
		if manager.m == nil {
			manager.RUnlock()
			manager.init(measure)
			manager.RLock()
		}
		dp := request.GetRequest().GetDataPoint()
		spec := request.GetRequest().GetDataPointSpec()
		for _, processor := range manager.processorList {
			dpWithEntity := newDataPointWithEntityValues(
				dp,
				request.GetEntityValues(),
				seriesID,
				shardID,
				spec,
				manager.m,
			)
			processor.Src() <- flow.NewStreamRecordWithTimestampPb(dpWithEntity, dp.GetTimestamp())
		}
	}()
}

func (manager *topNProcessorManager) register(topNSchema *databasev1.TopNAggregation) {
	manager.Lock()
	defer manager.Unlock()
	if manager.closed {
		return
	}
	exist := false
	for i := range manager.registeredTasks {
		if manager.registeredTasks[i].GetMetadata().GetName() == topNSchema.GetMetadata().GetName() {
			exist = true
			if manager.registeredTasks[i].GetMetadata().GetModRevision() < topNSchema.GetMetadata().GetModRevision() {
				prev := manager.registeredTasks[i]
				prevProcessors := manager.removeProcessors(prev)
				if err := manager.start(topNSchema); err != nil {
					manager.l.Err(err).Msg("fail to start the new processor")
					return
				}
				manager.registeredTasks[i] = topNSchema
				for _, processor := range prevProcessors {
					if err := processor.Close(); err != nil {
						manager.l.Err(err).Msg("fail to close the prev processor")
					}
				}
			}
		}
	}
	if exist {
		return
	}
	manager.registeredTasks = append(manager.registeredTasks, topNSchema)
	if err := manager.start(topNSchema); err != nil {
		manager.l.Err(err).Msg("fail to start processor")
	}
}

func (manager *topNProcessorManager) start(topNSchema *databasev1.TopNAggregation) error {
	if manager.m == nil {
		return nil
	}
	interval, err := timestamp.ParseDuration(manager.m.Interval)
	if err != nil {
		return errors.Wrapf(err, "invalid interval %s for measure %s", manager.m.Interval, manager.m.GetMetadata().GetName())
	}
	sortDirections := make([]modelv1.Sort, 0, 2)
	if topNSchema.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		sortDirections = append(sortDirections, modelv1.Sort_SORT_ASC, modelv1.Sort_SORT_DESC)
	} else {
		sortDirections = append(sortDirections, topNSchema.GetFieldValueSort())
	}

	processorList := make([]topNProcessor, len(sortDirections))
	for i, sortDirection := range sortDirections {
		srcCh := make(chan interface{})
		src, _ := sources.NewChannel(srcCh)
		name := strings.Join([]string{topNSchema.GetMetadata().Group, topNSchema.GetMetadata().Name, modelv1.Sort_name[int32(sortDirection)]}, "-")
		streamingFlow := streaming.New(name, src)

		filters, buildErr := manager.buildFilter(topNSchema.GetCriteria())
		if buildErr != nil {
			return buildErr
		}
		streamingFlow = streamingFlow.Filter(filters)

		mapper, fieldType, innerErr := manager.buildMapper(topNSchema.GetFieldName(), topNSchema.GetGroupByTagNames()...)
		if innerErr != nil {
			return innerErr
		}
		streamingFlow = streamingFlow.Map(mapper)

		if fieldType == databasev1.FieldType_FIELD_TYPE_FLOAT {
			processor := &topNStreamingProcessor[float64]{
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
				nodeID:        manager.nodeID,
				fieldType:     fieldType,
			}
			processorList[i] = processor.start()
		} else {
			processor := &topNStreamingProcessor[int64]{
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
				nodeID:        manager.nodeID,
				fieldType:     fieldType,
			}
			processorList[i] = processor.start()
		}
	}
	manager.processorList = append(manager.processorList, processorList...)
	return nil
}

func (manager *topNProcessorManager) removeProcessors(topNSchema *databasev1.TopNAggregation) []topNProcessor {
	var processors []topNProcessor
	var newList []topNProcessor
	for i := range manager.processorList {
		if manager.processorList[i].TopNSchema().GetMetadata().GetName() == topNSchema.GetMetadata().GetName() {
			processors = append(processors, manager.processorList[i])
		} else {
			newList = append(newList, manager.processorList[i])
		}
	}
	manager.processorList = newList
	return processors
}

func (manager *topNProcessorManager) buildFilter(criteria *modelv1.Criteria) (flow.UnaryFunc[bool], error) {
	// if criteria is nil, we handle all incoming elements
	if criteria == nil {
		return func(_ context.Context, _ any) bool {
			return true
		}, nil
	}

	f, err := logical.BuildSimpleTagFilter(criteria)
	if err != nil {
		return nil, err
	}

	return func(_ context.Context, request any) bool {
		tffws := request.(*dataPointWithEntityValues).GetTagFamilies()
		tagSpec := request.(*dataPointWithEntityValues).tagSpec
		ok, matchErr := f.Match(logical.TagFamiliesForWrite(tffws), tagSpec)
		if matchErr != nil {
			manager.l.Err(matchErr).Msg("fail to match criteria")
			return false
		}
		return ok
	}, nil
}

func (manager *topNProcessorManager) buildMapper(fieldName string, groupByNames ...string) (flow.UnaryFunc[any], databasev1.FieldType, error) {
	fieldIdx := slices.IndexFunc(manager.m.GetFields(), func(spec *databasev1.FieldSpec) bool {
		return spec.GetName() == fieldName
	})
	if fieldIdx == -1 {
		manager.l.Warn().Str("fieldName", fieldName).Str("measure", manager.m.Metadata.GetName()).
			Msg("TopNAggregation references removed field which no longer exists in schema, ignoring this TopNAggregation")
		return nil, databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, fmt.Errorf("field %s is not found in %s schema", fieldName, manager.m.Metadata.GetName())
	}
	fieldType := manager.m.GetFields()[fieldIdx].GetFieldType()
	// Validate that the field type is supported by TopN streaming (INT or FLOAT only)
	if fieldType != databasev1.FieldType_FIELD_TYPE_INT && fieldType != databasev1.FieldType_FIELD_TYPE_FLOAT {
		manager.l.Error().
			Str("fieldName", fieldName).
			Str("fieldType", fieldType.String()).
			Str("measure", manager.m.Metadata.GetName()).
			Msg("TopNAggregation field type must be INT or FLOAT")
		return nil, databasev1.FieldType_FIELD_TYPE_UNSPECIFIED,
			fmt.Errorf("field %s has unsupported type %s for TopNAggregation (must be INT or FLOAT)", fieldName, fieldType.String())
	}
	if len(groupByNames) == 0 {
		return func(_ context.Context, request any) any {
			dpWithEvs := request.(*dataPointWithEntityValues)
			return flow.Data{
				dpWithEvs.entityValues,
				"",
				dpWithEvs.fieldValue(fieldName, fieldType, manager.l),
				dpWithEvs.shardID,
				dpWithEvs.seriesID,
			}
		}, fieldType, nil
	}
	var removedTags []string
	for i := range groupByNames {
		_, _, tagSpec := pbv1.FindTagByName(manager.m.GetTagFamilies(), groupByNames[i])
		if tagSpec == nil {
			removedTags = append(removedTags, groupByNames[i])
		}
	}
	if len(removedTags) > 0 {
		if len(removedTags) == len(groupByNames) {
			manager.l.Warn().Strs("removedTags", removedTags).Str("measure", manager.m.Metadata.GetName()).
				Msg("TopNAggregation references removed tags which no longer exist in schema, all groupBy tags were removed")
			return nil, databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, fmt.Errorf("all groupBy tags [%s] no longer exist in %s schema, TopNAggregation is invalid",
				strings.Join(removedTags, ", "), manager.m.Metadata.GetName())
		}
		manager.l.Warn().Strs("removedTags", removedTags).Str("measure", manager.m.Metadata.GetName()).
			Msg("TopNAggregation references removed tags which no longer exist in schema, ignoring these tags")
	}
	return func(_ context.Context, request any) any {
		dpWithEvs := request.(*dataPointWithEntityValues)
		groupValues := make([]string, 0, len(groupByNames))
		for i := range groupByNames {
			tagValue := dpWithEvs.tagValue(groupByNames[i])
			groupValues = append(groupValues, Stringify(tagValue))
		}
		return flow.Data{
			dpWithEvs.entityValues,
			GroupName(groupValues),
			dpWithEvs.fieldValue(fieldName, fieldType, manager.l),
			dpWithEvs.shardID,
			dpWithEvs.seriesID,
		}
	}, fieldType, nil
}

// Stringify converts a TagValue to a string.
func Stringify(tagValue *modelv1.TagValue) string {
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

// GenerateTopNValueInt returns a new TopNValue[int64] instance.
func GenerateTopNValueInt() *TopNValue[int64] {
	v := topNValueIntPool.Get()
	if v == nil {
		return &TopNValue[int64]{}
	}
	return v
}

// ReleaseTopNValueInt releases a TopNValue[int64] instance.
func ReleaseTopNValueInt(bi *TopNValue[int64]) {
	bi.Reset()
	topNValueIntPool.Put(bi)
}

// GenerateTopNValueFloat returns a new TopNValue[float64] instance.
func GenerateTopNValueFloat() *TopNValue[float64] {
	v := topNValueFloatPool.Get()
	if v == nil {
		return &TopNValue[float64]{}
	}
	return v
}

// ReleaseTopNValueFloat releases a TopNValue[float64] instance.
func ReleaseTopNValueFloat(bi *TopNValue[float64]) {
	bi.Reset()
	topNValueFloatPool.Put(bi)
}

var (
	topNValueIntPool   = pool.Register[*TopNValue[int64]]("measure-topNValue-int")
	topNValueFloatPool = pool.Register[*TopNValue[float64]]("measure-topNValue-float")
)

func generateTopNValue[N aggregation.Number]() *TopNValue[N] {
	var n N
	switch any(n).(type) {
	case int64:
		return any(GenerateTopNValueInt()).(*TopNValue[N])
	case float64:
		return any(GenerateTopNValueFloat()).(*TopNValue[N])
	default:
		return &TopNValue[N]{}
	}
}

func releaseTopNValue[N aggregation.Number](v *TopNValue[N]) {
	var n N
	switch any(n).(type) {
	case int64:
		ReleaseTopNValueInt(any(v).(*TopNValue[int64]))
	case float64:
		ReleaseTopNValueFloat(any(v).(*TopNValue[float64]))
	}
}

// TopNValue represents the topN value.
type TopNValue[N aggregation.Number] struct {
	valueName       string
	entityTagNames  []string
	values          []N
	entities        [][]*modelv1.TagValue
	entityValues    [][]byte
	entityValuesBuf [][]byte
	buf             []byte
	firstValue      int64
	exponent        int16
	encodeType      encoding.EncodeType
}

func (t *TopNValue[N]) setMetadata(valueName string, entityTagNames []string) {
	t.valueName = valueName
	t.entityTagNames = entityTagNames
}

func (t *TopNValue[N]) addValue(value N, entityValues []*modelv1.TagValue) {
	entityValuesCopy := make([]*modelv1.TagValue, len(entityValues))
	copy(entityValuesCopy, entityValues)
	t.values = append(t.values, value)
	t.entities = append(t.entities, entityValuesCopy)
}

// Values returns the valueName, entityTagNames, values, and entities.
func (t *TopNValue[N]) Values() (string, []string, []N, [][]*modelv1.TagValue) {
	return t.valueName, t.entityTagNames, t.values, t.entities
}

// Reset resets the TopNValue.
func (t *TopNValue[N]) Reset() {
	t.valueName = ""
	t.entityTagNames = t.entityTagNames[:0]
	t.values = t.values[:0]
	for i := range t.entities {
		t.entities[i] = t.entities[i][:0]
	}
	t.entities = t.entities[:0]
	t.buf = t.buf[:0]
	t.encodeType = encoding.EncodeTypeUnknown
	t.firstValue = 0
	t.exponent = 0
	t.entityValuesBuf = t.entityValuesBuf[:0]
	t.entityValues = t.entityValues[:0]
}

func (t *TopNValue[N]) resizeEntityValues(size int) [][]byte {
	entityValues := t.entityValues
	if n := size - cap(entityValues); n > 0 {
		entityValues = append(entityValues[:cap(entityValues)], make([][]byte, n)...)
	}
	t.entityValues = entityValues[:size]
	return t.entityValues
}

func (t *TopNValue[N]) resizeEntities(size, entitySize int) [][]*modelv1.TagValue {
	entities := t.entities
	if n := size - cap(t.entities); n > 0 {
		entities = append(entities[:cap(entities)], make([][]*modelv1.TagValue, n)...)
	}
	t.entities = entities[:size]
	for i := range t.entities {
		entity := t.entities[i]
		if n := entitySize - cap(entity); n > 0 {
			entity = append(entity[:cap(entity)], make([]*modelv1.TagValue, n)...)
		}
		t.entities[i] = entity[:0]
	}
	return t.entities
}

func (t *TopNValue[N]) marshal(dst []byte) ([]byte, error) {
	if len(t.values) == 0 {
		return nil, errors.New("values is empty")
	}
	var n N
	switch any(n).(type) {
	case float64:
		return t.marshalFloat64(dst)
	default:
		return t.marshalInt64(dst)
	}
}

func (t *TopNValue[N]) marshalInt64(dst []byte) ([]byte, error) {
	dst = encoding.EncodeBytes(dst, convert.StringToBytes(t.valueName))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.entityTagNames)))
	for _, entityTagName := range t.entityTagNames {
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(entityTagName))
	}

	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.values)))

	intValues := make([]int64, len(t.values))
	for i, v := range t.values {
		intValues[i] = int64(v)
	}
	t.buf, t.encodeType, t.firstValue = encoding.Int64ListToBytes(t.buf, intValues)
	dst = append(dst, byte(t.encodeType))
	dst = encoding.VarInt64ToBytes(dst, t.firstValue)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.buf)))
	dst = append(dst, t.buf...)

	var err error
	evv := t.resizeEntityValues(len(t.entities))
	for i, tvv := range t.entities {
		ev := evv[i]
		ev, err = pbv1.MarshalTagValues(ev[:0], tvv)
		if err != nil {
			return nil, err
		}
		evv[i] = ev
	}
	dst = encoding.EncodeBytesBlock(dst, evv)
	return dst, nil
}

func (t *TopNValue[N]) marshalFloat64(dst []byte) ([]byte, error) {
	dst = encoding.EncodeBytes(dst, convert.StringToBytes(t.valueName))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.entityTagNames)))
	for _, entityTagName := range t.entityTagNames {
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(entityTagName))
	}

	valuesCount := uint64(len(t.values))
	if valuesCount&(1<<63) != 0 {
		return nil, errors.New("float values count overflow")
	}
	valuesCount |= (1 << 63)
	dst = encoding.VarUint64ToBytes(dst, valuesCount)

	floatValues := make([]float64, len(t.values))
	for i, v := range t.values {
		floatValues[i] = float64(v)
	}

	intValues, exponent, err := encoding.Float64ListToDecimalIntList(nil, floatValues)
	if err != nil {
		return nil, fmt.Errorf("failed to convert float64 to decimal int: %w", err)
	}
	t.exponent = exponent

	t.buf, t.encodeType, t.firstValue = encoding.Int64ListToBytes(t.buf, intValues)
	dst = append(dst, byte(t.encodeType))
	dst = encoding.VarInt64ToBytes(dst, t.firstValue)
	dst = encoding.VarInt64ToBytes(dst, int64(t.exponent))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.buf)))
	dst = append(dst, t.buf...)

	evv := t.resizeEntityValues(len(t.entities))
	for i, tvv := range t.entities {
		ev := evv[i]
		ev, err = pbv1.MarshalTagValues(ev[:0], tvv)
		if err != nil {
			return nil, err
		}
		evv[i] = ev
	}
	dst = encoding.EncodeBytesBlock(dst, evv)
	return dst, nil
}

// DetectFieldTypeFromBinary detects the field type from binary data.
func DetectFieldTypeFromBinary(src []byte) (databasev1.FieldType, error) {
	var err error
	src, _, err = encoding.DecodeBytes(src)
	if err != nil {
		return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, err
	}
	var count uint64
	src, count = encoding.BytesToVarUint64(src)
	for i := uint64(0); i < count; i++ {
		src, _, err = encoding.DecodeBytes(src)
		if err != nil {
			return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, err
		}
	}
	_, count = encoding.BytesToVarUint64(src)
	if (count & (1 << 63)) != 0 {
		return databasev1.FieldType_FIELD_TYPE_FLOAT, nil
	}
	return databasev1.FieldType_FIELD_TYPE_INT, nil
}

// MergeTopNBinaryValues merges two TopN binary values and returns the merged binary value.
func MergeTopNBinaryValues(
	left, right []byte, topN int32, sort modelv1.Sort, decoder *encoding.BytesBlockDecoder,
	timestamp uint64, leftVersion, rightVersion int64,
) ([]byte, error) {
	fieldType, detectErr := DetectFieldTypeFromBinary(left)
	if detectErr != nil {
		fieldType, detectErr = DetectFieldTypeFromBinary(right)
		if detectErr != nil {
			return nil, fmt.Errorf("failed to detect field type from both sides: %w", detectErr)
		}
	}

	if fieldType == databasev1.FieldType_FIELD_TYPE_FLOAT {
		topNPostAggregator := CreateTopNPostProcessorFloat(topN, modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED, sort)
		return mergeTopNBinaryValues[float64](left, right, topNPostAggregator, decoder, timestamp, leftVersion, rightVersion)
	}
	topNPostAggregator := CreateTopNPostProcessorInt(topN, modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED, sort)
	return mergeTopNBinaryValues[int64](left, right, topNPostAggregator, decoder, timestamp, leftVersion, rightVersion)
}

func mergeTopNBinaryValues[N aggregation.Number](
	left, right []byte, topNPostAggregator PostProcessor[N], decoder *encoding.BytesBlockDecoder,
	timestamp uint64, leftVersion, rightVersion int64,
) ([]byte, error) {
	topNValue := generateTopNValue[N]()
	defer releaseTopNValue(topNValue)

	topNPostAggregator.Reset()

	var valueName string
	var entityTagNames []string
	hasValidData := false

	if err := topNValue.Unmarshal(left, decoder); err != nil {
		log.Warn().Err(err).Msg("failed to unmarshal left topN value, ignoring left side")
	} else {
		valueName = topNValue.valueName
		entityTagNames = topNValue.entityTagNames
		putEntitiesToAggregator(topNValue, topNPostAggregator, timestamp, leftVersion)
		hasValidData = true
	}

	topNValue.Reset()
	if err := topNValue.Unmarshal(right, decoder); err != nil {
		log.Warn().Err(err).Msg("failed to unmarshal right topN value, ignoring right side")
	} else {
		if !hasValidData {
			valueName = topNValue.valueName
			entityTagNames = topNValue.entityTagNames
		}
		putEntitiesToAggregator(topNValue, topNPostAggregator, timestamp, rightVersion)
		hasValidData = true
	}

	if !hasValidData {
		return []byte{}, nil
	}

	topNValue.Reset()
	topNValue.setMetadata(valueName, entityTagNames)

	items, err := topNPostAggregator.Flush()
	if err != nil {
		return nil, fmt.Errorf("failed to flush aggregator: %w", err)
	}

	for _, item := range items {
		topNValue.addValue(item.val, item.values)
	}

	return topNValue.marshal(make([]byte, 0, 128))
}

func putEntitiesToAggregator[N aggregation.Number](topNValue *TopNValue[N], aggregator PostProcessor[N], timestamp uint64, version int64) {
	for i, entityList := range topNValue.entities {
		entityValues := make(pbv1.EntityValues, len(entityList))
		copy(entityValues, entityList)
		aggregator.Put(entityValues, topNValue.values[i], timestamp, version)
	}
}

// Unmarshal unmarshals the TopNValue from the src.
func (t *TopNValue[N]) Unmarshal(src []byte, decoder *encoding.BytesBlockDecoder) error {
	var err error
	src, nameBytes, err := encoding.DecodeBytes(src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.name: %w", err)
	}
	t.valueName = convert.BytesToString(nameBytes)

	var entityTagNamesCount uint64
	src, entityTagNamesCount = encoding.BytesToVarUint64(src)
	t.entityTagNames = make([]string, 0, entityTagNamesCount)
	var entityTagNameBytes []byte
	for i := uint64(0); i < entityTagNamesCount; i++ {
		src, entityTagNameBytes, err = encoding.DecodeBytes(src)
		if err != nil {
			return fmt.Errorf("cannot unmarshal topNValue.entityTagName: %w", err)
		}
		t.entityTagNames = append(t.entityTagNames, convert.BytesToString(entityTagNameBytes))
	}

	var valuesCount uint64
	src, valuesCount = encoding.BytesToVarUint64(src)

	var n N
	switch any(n).(type) {
	case float64:
		valuesCount &^= (1 << 63)
		return t.unmarshalFloat64(src, decoder, valuesCount, int(entityTagNamesCount))
	default:
		return t.unmarshalInt64(src, decoder, valuesCount, int(entityTagNamesCount))
	}
}

func (t *TopNValue[N]) unmarshalInt64(src []byte, decoder *encoding.BytesBlockDecoder, valuesCount uint64, entityTagNamesCount int) error {
	var err error
	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.encodeType: src is too short")
	}
	t.encodeType = encoding.EncodeType(src[0])
	src = src[1:]

	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.firstValue: src is too short")
	}
	src, t.firstValue, err = encoding.BytesToVarInt64(src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.firstValue: %w", err)
	}
	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.valueLen: src is too short")
	}
	var valueLen uint64
	src, valueLen = encoding.BytesToVarUint64(src)

	if uint64(len(src)) < valueLen {
		return fmt.Errorf("src is too short for reading string with size %d; len(src)=%d", valueLen, len(src))
	}

	intValues, err := encoding.BytesToInt64List(nil, src[:valueLen], t.encodeType, t.firstValue, int(valuesCount))
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.values: %w", err)
	}
	t.values = make([]N, len(intValues))
	for i, v := range intValues {
		t.values[i] = N(v)
	}

	decoder.Reset()
	evv := t.entityValuesBuf
	evv, err = decoder.Decode(evv[:0], src[valueLen:], valuesCount)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.entityValues: %w", err)
	}
	t.resizeEntities(len(evv), entityTagNamesCount)
	for i, ev := range evv {
		t.buf, t.entities[i], err = pbv1.UnmarshalTagValues(t.buf, t.entities[i], ev)
		if err != nil {
			return fmt.Errorf("cannot unmarshal topNValue.entityValues[%d]:%s %w", i, hex.EncodeToString(ev), err)
		}
		if len(t.entities[i]) != len(t.entityTagNames) {
			return fmt.Errorf("entityValues[%d] length is not equal to entityTagNames", i)
		}
	}
	return nil
}

func (t *TopNValue[N]) unmarshalFloat64(src []byte, decoder *encoding.BytesBlockDecoder, valuesCount uint64, entityTagNamesCount int) error {
	var err error
	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.encodeType: src is too short")
	}
	t.encodeType = encoding.EncodeType(src[0])
	src = src[1:]

	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.firstValue: src is too short")
	}
	src, t.firstValue, err = encoding.BytesToVarInt64(src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.firstValue: %w", err)
	}

	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.exponent: src is too short")
	}
	var exp int64
	src, exp, err = encoding.BytesToVarInt64(src)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.exponent: %w", err)
	}
	t.exponent = int16(exp)

	if len(src) < 1 {
		return fmt.Errorf("cannot unmarshal topNValue.valueLen: src is too short")
	}
	var valueLen uint64
	src, valueLen = encoding.BytesToVarUint64(src)

	if uint64(len(src)) < valueLen {
		return fmt.Errorf("src is too short for reading float values with size %d; len(src)=%d", valueLen, len(src))
	}

	intValues, err := encoding.BytesToInt64List(nil, src[:valueLen], t.encodeType, t.firstValue, int(valuesCount))
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.intValues: %w", err)
	}

	floatValues, err := encoding.DecimalIntListToFloat64List(nil, intValues, t.exponent, int(valuesCount))
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.floatValues: %w", err)
	}
	t.values = make([]N, len(floatValues))
	for i, v := range floatValues {
		t.values[i] = N(v)
	}

	decoder.Reset()
	evv := t.entityValuesBuf
	evv, err = decoder.Decode(evv[:0], src[valueLen:], valuesCount)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.entityValues: %w", err)
	}
	t.resizeEntities(len(evv), entityTagNamesCount)
	for i, ev := range evv {
		t.buf, t.entities[i], err = pbv1.UnmarshalTagValues(t.buf, t.entities[i], ev)
		if err != nil {
			return fmt.Errorf("cannot unmarshal topNValue.entityValues[%d]:%s %w", i, hex.EncodeToString(ev), err)
		}
		if len(t.entities[i]) != len(t.entityTagNames) {
			return fmt.Errorf("entityValues[%d] length is not equal to entityTagNames", i)
		}
	}
	return nil
}

// GroupName returns the group name.
func GroupName(groupTags []string) string {
	return strings.Join(groupTags, "|")
}

// GenerateTopNValuesDecoder returns a new decoder instance of TopNValues.
func GenerateTopNValuesDecoder() *encoding.BytesBlockDecoder {
	v := topNValuesDecoderPool.Get()
	if v == nil {
		return &encoding.BytesBlockDecoder{}
	}
	return v
}

// ReleaseTopNValuesDecoder releases a decoder instance of TopNValues.
func ReleaseTopNValuesDecoder(d *encoding.BytesBlockDecoder) {
	d.Reset()
	topNValuesDecoderPool.Put(d)
}

var topNValuesDecoderPool = pool.Register[*encoding.BytesBlockDecoder]("topn-valueDecoder")
