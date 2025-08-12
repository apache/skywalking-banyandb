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
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	timeBucketFormat         = "200601021504"
	resultPersistencyTimeout = 10 * time.Second
	maxFlushInterval         = time.Minute
)

var (
	_ io.Closer = (*topNStreamingProcessor)(nil)
	_ io.Closer = (*topNProcessorManager)(nil)
	_ flow.Sink = (*topNStreamingProcessor)(nil)
)

func (sr *schemaRepo) inFlow(stm *databasev1.Measure, seriesID uint64, shardID uint32, entityValues []*modelv1.TagValue, dp *measurev1.DataPointValue) {
	if p, _ := sr.topNProcessorMap.Load(getKey(stm.GetMetadata())); p != nil {
		p.(*topNProcessorManager).onMeasureWrite(seriesID, shardID, &measurev1.InternalWriteRequest{
			Request: &measurev1.WriteRequest{
				Metadata:  stm.GetMetadata(),
				DataPoint: dp,
				MessageId: uint64(time.Now().UnixNano()),
			},
			EntityValues: entityValues,
		}, stm)
	}
}

func (sr *schemaRepo) getSteamingManager(source *commonv1.Metadata, pipeline queue.Client) (manager *topNProcessorManager) {
	key := getKey(source)
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
		if pre.m.GetMetadata().GetModRevision() < sourceMeasure.schema.GetMetadata().GetModRevision() {
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
	*measurev1.DataPointValue
	entityValues []*modelv1.TagValue
	seriesID     uint64
	shardID      uint32
}

type topNStreamingProcessor struct {
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

func (t *topNStreamingProcessor) writeStreamRecord(record flow.StreamRecord, buf []byte) error {
	tuplesGroups, ok := record.Data().(map[string][]*streaming.Tuple2)
	if !ok {
		return errors.New("invalid data type")
	}
	// down-sample the start of the timeWindow to a time-bucket
	eventTime := t.downSampleTimeBucket(record.TimestampMillis())
	var err error
	publisher := t.pipeline.NewBatchPublisher(resultPersistencyTimeout)
	defer publisher.Close()
	topNValue := GenerateTopNValue()
	defer ReleaseTopNValue(topNValue)
	for group, tuples := range tuplesGroups {
		if e := t.l.Debug(); e.Enabled() {
			for i := range tuples {
				tuple := tuples[i]
				data := tuple.V2.(flow.StreamRecord).Data().(flow.Data)
				e.
					Int("rankNums", i+1).
					Str("entityValues", fmt.Sprintf("%v", data[0])).
					Int("value", int(data[2].(int64))).
					Time("eventTime", eventTime).
					Msgf("Write tuples %s %s", t.topNSchema.GetMetadata().GetName(), group)
			}
		}
		topNValue.Reset()
		topNValue.setMetadata(t.topNSchema.GetFieldName(), t.m.Entity.TagNames)
		var shardID uint32
		for _, tuple := range tuples {
			data := tuple.V2.(flow.StreamRecord).Data().(flow.Data)
			topNValue.addValue(
				tuple.V1.(int64),
				data[0].([]*modelv1.TagValue),
			)
			shardID = data[3].(uint32)
		}
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
						Value: t.nodeID,
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

func (t *topNStreamingProcessor) downSampleTimeBucket(eventTimeMillis int64) time.Time {
	return time.UnixMilli(eventTimeMillis - eventTimeMillis%t.interval.Milliseconds())
}

func (t *topNStreamingProcessor) start() *topNStreamingProcessor {
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
	pipeline        queue.Client
	s               logical.TagSpecRegistry
	l               *logger.Logger
	m               *databasev1.Measure
	nodeID          string
	registeredTasks []*databasev1.TopNAggregation
	processorList   []*topNStreamingProcessor
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
	tagMapSpec := logical.TagSpecMap{}
	tagMapSpec.RegisterTagFamilies(m.GetTagFamilies())
	manager.s = tagMapSpec
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
	manager.s = nil
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
		for _, processor := range manager.processorList {
			processor.src <- flow.NewStreamRecordWithTimestampPb(&dataPointWithEntityValues{
				request.GetRequest().GetDataPoint(),
				request.GetEntityValues(),
				seriesID,
				shardID,
			}, request.GetRequest().GetDataPoint().GetTimestamp())
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

	processorList := make([]*topNStreamingProcessor, len(sortDirections))
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
			nodeID:        manager.nodeID,
		}
		processorList[i] = processor.start()
	}
	manager.processorList = append(manager.processorList, processorList...)
	return nil
}

func (manager *topNProcessorManager) removeProcessors(topNSchema *databasev1.TopNAggregation) []*topNStreamingProcessor {
	var processors []*topNStreamingProcessor
	var newList []*topNStreamingProcessor
	for i := range manager.processorList {
		if manager.processorList[i].topNSchema.GetMetadata().GetName() == topNSchema.GetMetadata().GetName() {
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
		ok, matchErr := f.Match(logical.TagFamiliesForWrite(tffws), manager.s)
		if matchErr != nil {
			manager.l.Err(matchErr).Msg("fail to match criteria")
			return false
		}
		return ok
	}, nil
}

func (manager *topNProcessorManager) buildMapper(fieldName string, groupByNames ...string) (flow.UnaryFunc[any], error) {
	fieldIdx := slices.IndexFunc(manager.m.GetFields(), func(spec *databasev1.FieldSpec) bool {
		return spec.GetName() == fieldName
	})
	if fieldIdx == -1 {
		return nil, fmt.Errorf("field %s is not found in %s schema", fieldName, manager.m.Metadata.GetName())
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
				dpWithEvs.GetFields()[fieldIdx].GetInt().GetValue(),
				// shardID values as v3
				dpWithEvs.shardID,
				// seriesID values as v4
				dpWithEvs.seriesID,
			}
		}, nil
	}
	groupLocator, err := newGroupLocator(manager.m, groupByNames)
	if err != nil {
		return nil, err
	}
	return func(_ context.Context, request any) any {
		dpWithEvs := request.(*dataPointWithEntityValues)
		return flow.Data{
			// EntityValues as identity
			dpWithEvs.entityValues,
			// save string representation of group values as the key, i.e. v1
			GroupName(transform(groupLocator, func(locator partition.TagLocator) string {
				return Stringify(extractTagValue(dpWithEvs.DataPointValue, locator))
			})),
			// field value as v2
			dpWithEvs.GetFields()[fieldIdx].GetInt().GetValue(),
			// shardID values as v3
			dpWithEvs.shardID,
			// seriesID values as v4
			dpWithEvs.seriesID,
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

// GenerateTopNValue returns a new TopNValue instance.
func GenerateTopNValue() *TopNValue {
	v := topNValuePool.Get()
	if v == nil {
		return &TopNValue{}
	}
	return v
}

// ReleaseTopNValue releases a TopNValue instance.
func ReleaseTopNValue(bi *TopNValue) {
	bi.Reset()
	topNValuePool.Put(bi)
}

var topNValuePool = pool.Register[*TopNValue]("measure-topNValue")

// TopNValue represents the topN value.
type TopNValue struct {
	valueName       string
	entityTagNames  []string
	values          []int64
	entities        [][]*modelv1.TagValue
	entityValues    [][]byte
	entityValuesBuf [][]byte
	buf             []byte
	encodeType      encoding.EncodeType
	firstValue      int64
}

func (t *TopNValue) setMetadata(valueName string, entityTagNames []string) {
	t.valueName = valueName
	t.entityTagNames = entityTagNames
}

func (t *TopNValue) addValue(value int64, entityValues []*modelv1.TagValue) {
	t.values = append(t.values, value)
	t.entities = append(t.entities, entityValues)
}

// Values returns the valueName, entityTagNames, values, and entities.
func (t *TopNValue) Values() (string, []string, []int64, [][]*modelv1.TagValue) {
	return t.valueName, t.entityTagNames, t.values, t.entities
}

// Reset resets the TopNValue.
func (t *TopNValue) Reset() {
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
	t.entityValuesBuf = t.entityValuesBuf[:0]
	t.entityValues = t.entityValues[:0]
}

func (t *TopNValue) resizeEntityValues(size int) [][]byte {
	entityValues := t.entityValues
	if n := size - cap(entityValues); n > 0 {
		entityValues = append(entityValues[:cap(entityValues)], make([][]byte, n)...)
	}
	t.entityValues = entityValues[:size]
	return t.entityValues
}

func (t *TopNValue) resizeEntities(size int, entitySize int) [][]*modelv1.TagValue {
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

func (t *TopNValue) marshal(dst []byte) ([]byte, error) {
	if len(t.values) == 0 {
		return nil, errors.New("values is empty")
	}
	dst = encoding.EncodeBytes(dst, convert.StringToBytes(t.valueName))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.entityTagNames)))
	for _, entityTagName := range t.entityTagNames {
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(entityTagName))
	}

	dst = encoding.VarUint64ToBytes(dst, uint64(len(t.values)))

	t.buf, t.encodeType, t.firstValue = encoding.Int64ListToBytes(t.buf, t.values)
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

// Unmarshal unmarshals the TopNValue from the src.
func (t *TopNValue) Unmarshal(src []byte, decoder *encoding.BytesBlockDecoder) error {
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

	t.values, err = encoding.BytesToInt64List(t.values, src[:valueLen], t.encodeType, t.firstValue, int(valuesCount))
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.values: %w", err)
	}

	if uint64(len(src)) < valueLen {
		return fmt.Errorf("src is too short for reading string with size %d; len(src)=%d", valueLen, len(src))
	}

	decoder.Reset()
	evv := t.entityValuesBuf
	evv, err = decoder.Decode(evv[:0], src[valueLen:], valuesCount)
	if err != nil {
		return fmt.Errorf("cannot unmarshal topNValue.entityValues: %w", err)
	}
	t.resizeEntities(len(evv), int(entityTagNamesCount))
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
