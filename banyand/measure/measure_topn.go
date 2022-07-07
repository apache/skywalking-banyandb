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
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	_ bus.MessageListener = (*topNProcessCallback)(nil)
	_ io.Closer           = (*topNProcessor)(nil)
	_ io.Closer           = (*topNProcessorManager)(nil)
)

type measureProcessorFilter func(request *measurev1.DataPointValue) bool
type measureProcessorMapper func(request *measurev1.DataPointValue) api.Data

type topNProcessor struct {
	input         chan interface{}
	errCh         <-chan error
	streamingFlow api.Flow
}

func (t *topNProcessor) Close() error {
	close(t.input)
	return nil
}

// topNProcessorManager manages multiple topNProcessor(s) belonging to a single measure
type topNProcessorManager struct {
	l            *logger.Logger
	schema       *databasev1.Measure
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
		processor.input <- api.NewStreamRecordWithTimestampPb(request.GetDataPoint(), request.GetDataPoint().GetTimestamp())
	}

	return nil
}

func (manager *topNProcessorManager) start() error {
	for _, topNSchema := range manager.topNSchemas {
		input := make(chan interface{})
		streamingFlow := streaming.New(input)

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

		// TODO: how to set size and slide
		errCh := streamingFlow.Window(streaming.NewSlidingTimeWindows(60*time.Second, 25*time.Second)).
			TopN(int(topNSchema.GetCountersNumber()),
				streaming.WithSortKeyExtractor(func(elem interface{}) int64 {
					return elem.(api.Data)[1].(int64)
				}),
						streaming.WithCacheSize(int(topNSchema.GetCountersNumber()))).
			To(nil).OpenAsync() // TODO: add destination impl

		manager.processorMap[topNSchema.GetSourceMeasure()] = &topNProcessor{
			input:         input,
			errCh:         errCh,
			streamingFlow: streamingFlow,
		}
	}

	return nil
}

func (manager *topNProcessorManager) buildFilters(criteria []*modelv1.Criteria) (measureProcessorFilter, error) {
	var filters []conditionFilter
	for _, group := range criteria {
		tagFamilyName := group.GetTagFamilyName()
		tagFamilyIdx := slices.IndexFunc(manager.schema.GetTagFamilies(), func(spec *databasev1.TagFamilySpec) bool {
			return spec.GetName() == tagFamilyName
		})
		if tagFamilyIdx == -1 {
			return nil, errors.New("invalid condition: tag family not found")
		}
		tagSpecs := manager.schema.GetTagFamilies()[tagFamilyIdx].GetTags()
		for _, cond := range group.GetConditions() {
			tagName := cond.GetName()
			tagIdx := slices.IndexFunc(tagSpecs, func(spec *databasev1.TagSpec) bool {
				return spec.GetName() == tagName
			})
			if tagIdx == -1 {
				return nil, errors.New("invalid condition: tag not found")
			}
			filters = append(filters, manager.buildFilterForTag(tagFamilyIdx, tagIdx, cond))
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

func (manager *topNProcessorManager) buildFilterForTag(tagFamilyIdx, tagIdx int, cond *modelv1.Condition) conditionFilter {
	switch v := cond.GetValue().GetValue().(type) {
	case *modelv1.TagValue_Int:
		return &int64TagFilter{
			tagLocator: &tagLocator{
				tagFamilyIdx: tagFamilyIdx,
				tagIdx:       tagIdx,
			},
			op:  cond.GetOp(),
			val: v.Int.GetValue(),
		}
	case *modelv1.TagValue_Str:
		return &strTagFilter{
			tagLocator: &tagLocator{
				tagFamilyIdx: tagFamilyIdx,
				tagIdx:       tagIdx,
			},
			op:  cond.GetOp(),
			val: v.Str.GetValue(),
		}
	case *modelv1.TagValue_Id:
		return &idTagFilter{
			tagLocator: &tagLocator{
				tagFamilyIdx: tagFamilyIdx,
				tagIdx:       tagIdx,
			},
			op:  cond.GetOp(),
			val: v.Id.GetValue(),
		}
	default:
		return nil
	}
}

func (manager *topNProcessorManager) buildMapper(fieldName string, groupByNames ...string) (measureProcessorMapper, error) {
	fieldIdx := slices.IndexFunc(manager.schema.GetFields(), func(spec *databasev1.FieldSpec) bool {
		return spec.GetName() == fieldName
	})
	if fieldIdx == -1 {
		return nil, errors.New("invalid fieldName")
	}
	gMapper, err := newGroupMapper(manager.schema, groupByNames)
	if err != nil {
		return nil, err
	}
	return func(request *measurev1.DataPointValue) api.Data {
		return api.Row(
			gMapper.transform(request.GetTagFamilies()),
			// TODO we only support
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

type tagLocator struct {
	tagFamilyIdx int
	tagIdx       int
}

type strTagFilter struct {
	*tagLocator
	op  modelv1.Condition_BinaryOp
	val string
}

func (f *strTagFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	strValue := tagFamilies[f.tagFamilyIdx].GetTags()[f.tagIdx].GetStr().GetValue()
	switch f.op {
	case modelv1.Condition_BINARY_OP_EQ:
		return strValue == f.val
	case modelv1.Condition_BINARY_OP_NE:
		return strValue != f.val
	}
	return false
}

type idTagFilter struct {
	*tagLocator
	op  modelv1.Condition_BinaryOp
	val string
}

func (f *idTagFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	val := tagFamilies[f.tagFamilyIdx].GetTags()[f.tagIdx].GetId().GetValue()
	switch f.op {
	case modelv1.Condition_BINARY_OP_EQ:
		return val == f.val
	case modelv1.Condition_BINARY_OP_NE:
		return val != f.val
	}
	return false
}

type int64TagFilter struct {
	*tagLocator
	op  modelv1.Condition_BinaryOp
	val int64
}

func (f *int64TagFilter) predicate(tagFamilies []*modelv1.TagFamilyForWrite) bool {
	val := tagFamilies[f.tagFamilyIdx].GetTags()[f.tagIdx].GetInt().GetValue()
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

type groupMapper struct {
	groupTags []*tagLocator
}

func newGroupMapper(m *databasev1.Measure, groupByNames []string) (*groupMapper, error) {
	groupTags := make([]*tagLocator, len(groupByNames))
	for idx, groupByName := range groupByNames {
		locator, err := findTagByName(m, groupByName)
		if err != nil {
			return nil, err
		}
		groupTags[idx] = locator
	}
	return &groupMapper{
		groupTags: groupTags,
	}, nil
}

func findTagByName(m *databasev1.Measure, name string) (*tagLocator, error) {
	for i, tagFamily := range m.GetTagFamilies() {
		for j, tag := range tagFamily.GetTags() {
			if tag.GetName() == name {
				return &tagLocator{
					tagFamilyIdx: i,
					tagIdx:       j,
				}, nil
			}
		}
	}
	return nil, errors.New("tag not found")
}

func (m *groupMapper) transform(tagFamilies []*modelv1.TagFamilyForWrite) string {
	bld := &strings.Builder{}
	for i, tag := range m.groupTags {
		bld.WriteString(stringify(tagFamilies[tag.tagFamilyIdx].GetTags()[tag.tagIdx]))
		if i != len(m.groupTags)-1 {
			// append a separator
			bld.WriteString("|")
		}
	}
	return bld.String()
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
