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
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	_ bus.MessageListener = (*topNProcessorManager)(nil)
)

type topNProcessor struct {
	input         chan interface{}
	errCh         <-chan error
	streamingFlow api.Flow
}

type topNProcessorManager struct {
	l            *logger.Logger
	schemaRepo   *schemaRepo
	processorMap map[*commonv1.Metadata]*topNProcessor
}

func (cb *topNProcessorManager) start() error {
	// load all topNAggregations and start processor
	topNSchemas, err := cb.schemaRepo.metadata.TopNAggregationRegistry().ListTopNAggregation(context.TODO(), schema.ListOpt{})
	if err != nil {
		return err
	}

	for _, topNSchema := range topNSchemas {
		m, innerErr := cb.schemaRepo.metadata.MeasureRegistry().GetMeasure(context.TODO(), topNSchema.GetSourceMeasure())
		if err != nil {
			return innerErr
		}
		input := make(chan interface{})
		streamingFlow := streaming.New(input)

		if conditions := topNSchema.GetCriteria(); len(conditions) > 0 {
			filters, buildErr := cb.buildFilters(m, conditions)
			if buildErr != nil {
				return buildErr
			}
			streamingFlow = streamingFlow.Filter(filters)
		}

		mapper, innerErr := cb.buildMapper(m, topNSchema.GetFieldName(), topNSchema.GetGroupByTagNames()...)
		if err != nil {
			return innerErr
		}
		streamingFlow = streamingFlow.Map(mapper)

		// TODO: how to set size and slide
		errCh := streamingFlow.Window(streaming.NewSlidingTimeWindows(60*time.Second, 25*time.Second)).
			TopN(10, topNSchema.GetCountersNumber()).
			To(nil).OpenAsync() // TODO: add destination impl

		cb.processorMap[topNSchema.GetSourceMeasure()] = &topNProcessor{
			input:         input,
			errCh:         errCh,
			streamingFlow: streamingFlow,
		}
	}

	return nil
}

func (cb *topNProcessorManager) buildFilters(m *databasev1.Measure, criteria []*modelv1.Criteria) (func(request *measurev1.WriteRequest) bool, error) {
	var filters []conditionFilter
	for _, group := range criteria {
		tagFamilyName := group.GetTagFamilyName()
		tagFamilyIdx := slices.IndexFunc(m.GetTagFamilies(), func(spec *databasev1.TagFamilySpec) bool {
			return spec.GetName() == tagFamilyName
		})
		if tagFamilyIdx == -1 {
			return nil, errors.New("invalid condition: tag family not found")
		}
		tagSpecs := m.GetTagFamilies()[tagFamilyIdx].GetTags()
		for _, cond := range group.GetConditions() {
			tagName := cond.GetName()
			tagIdx := slices.IndexFunc(tagSpecs, func(spec *databasev1.TagSpec) bool {
				return spec.GetName() == tagName
			})
			if tagIdx == -1 {
				return nil, errors.New("invalid condition: tag not found")
			}
			filters = append(filters, cb.buildFilterForTag(tagFamilyIdx, tagIdx, cond))
		}
	}

	if len(filters) == 0 {
		return func(*measurev1.WriteRequest) bool {
			return true
		}, nil
	}

	return func(request *measurev1.WriteRequest) bool {
		tfs := request.GetDataPoint().GetTagFamilies()
		for _, f := range filters {
			if !f.predicate(tfs) {
				return false
			}
		}
		return true
	}, nil
}

func (cb *topNProcessorManager) buildFilterForTag(tagFamilyIdx, tagIdx int, cond *modelv1.Condition) conditionFilter {
	switch v := cond.GetValue().GetValue().(type) {
	case *modelv1.TagValue_Int:
		return &int64TagFilter{
			tagFamilyIdx: tagFamilyIdx,
			tagIdx:       tagIdx,
			op:           cond.GetOp(),
			val:          v.Int.GetValue(),
		}
	case *modelv1.TagValue_Str:
		return &strTagFilter{
			tagFamilyIdx: tagFamilyIdx,
			tagIdx:       tagIdx,
			op:           cond.GetOp(),
			val:          v.Str.GetValue(),
		}
	case *modelv1.TagValue_Id:
		return &idTagFilter{
			tagFamilyIdx: tagFamilyIdx,
			tagIdx:       tagIdx,
			op:           cond.GetOp(),
			val:          v.Id.GetValue(),
		}
	default:
		return nil
	}
}

func (cb *topNProcessorManager) buildMapper(m *databasev1.Measure, fieldName string, groupByNames ...string) (func(request *measurev1.WriteRequest) streaming.Tuple2, error) {
	fieldIdx := slices.IndexFunc(m.GetFields(), func(spec *databasev1.FieldSpec) bool {
		return spec.GetName() == fieldName
	})
	if fieldIdx == -1 {
		return nil, errors.New("invalid fieldName")
	}
	var groupByIndices []int
	if len(groupByNames) > 0 {
		groupByIndices = append(groupByIndices, 0)
	}
	return func(request *measurev1.WriteRequest) streaming.Tuple2 {
		return streaming.Tuple2{
			First:  nil,
			Second: request.GetDataPoint().GetFields()[fieldIdx],
		}
	}, nil
}

func (cb *topNProcessorManager) Rev(message bus.Message) (resp bus.Message) {
	writeEvent, ok := message.Data().(*measurev1.InternalWriteRequest)
	if !ok {
		cb.l.Warn().Msg("invalid event data type")
		return
	}

	// first check processor existence
	processor, ok := cb.processorMap[writeEvent.GetRequest().GetMetadata()]
	if !ok {
		cb.l.Warn().Msg("fail to find processor for measure entity")
		return
	}

	processor.input <- api.NewStreamRecordWithTimestampPb(writeEvent.GetRequest(), writeEvent.GetRequest().GetDataPoint().GetTimestamp())
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
	tagFamilyIdx int
	tagIdx       int
	op           modelv1.Condition_BinaryOp
	val          string
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
	tagFamilyIdx int
	tagIdx       int
	op           modelv1.Condition_BinaryOp
	val          string
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
	tagFamilyIdx int
	tagIdx       int
	op           modelv1.Condition_BinaryOp
	val          int64
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
