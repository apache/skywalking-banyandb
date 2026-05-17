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
	"container/list"
	"context"
	"fmt"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

const defaultQueryTimeout = 15 * time.Second

var _ logical.UnresolvedPlan = (*unresolvedDistributed)(nil)

type pushDownAggSchema struct {
	originalSchema   logical.Schema
	aggregationField *logical.Field
}

func (as *pushDownAggSchema) CreateFieldRef(fields ...*logical.Field) ([]*logical.FieldRef, error) {
	originalRefs, err := as.originalSchema.CreateFieldRef(fields...)
	if err != nil {
		return nil, err
	}
	for _, ref := range originalRefs {
		if ref.Spec != nil {
			ref.Spec.FieldIdx = 0
		}
	}
	return originalRefs, nil
}

func (as *pushDownAggSchema) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return as.originalSchema.CreateTagRef(tags...)
}

func (as *pushDownAggSchema) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
	return &pushDownAggSchema{
		originalSchema:   as.originalSchema.ProjTags(refs...),
		aggregationField: as.aggregationField,
	}
}

func (as *pushDownAggSchema) ProjFields(fieldRefs ...*logical.FieldRef) logical.Schema {
	return &pushDownAggSchema{
		originalSchema:   as.originalSchema.ProjFields(fieldRefs...),
		aggregationField: as.aggregationField,
	}
}

func (as *pushDownAggSchema) FindTagSpecByName(name string) *logical.TagSpec {
	return as.originalSchema.FindTagSpecByName(name)
}

func (as *pushDownAggSchema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return as.originalSchema.IndexDefined(tagName)
}

func (as *pushDownAggSchema) IndexRuleDefined(ruleName string) (bool, *databasev1.IndexRule) {
	return as.originalSchema.IndexRuleDefined(ruleName)
}

func (as *pushDownAggSchema) EntityList() []string {
	return as.originalSchema.EntityList()
}

func (as *pushDownAggSchema) Children() []logical.Schema {
	return as.originalSchema.Children()
}

// Deprecated: row-path measure plan; see .omc/g8-plan.md. Distributed
// vec query is a G8 follow-up; for now the dispatcher continues to use
// this row-path distributed plan even when vec is enabled on standalone.
type unresolvedDistributed struct {
	originalQuery *measurev1.QueryRequest
	groupByEntity bool
	pushDownAgg   bool
}

func newUnresolvedDistributed(query *measurev1.QueryRequest, pushDownAgg bool) logical.UnresolvedPlan {
	return &unresolvedDistributed{
		originalQuery: query,
		pushDownAgg:   pushDownAgg,
	}
}

func (ud *unresolvedDistributed) Analyze(s logical.Schema) (logical.Plan, error) {
	indexMode := s.(*schema).measure.IndexMode
	projectionTags := logical.ToTags(ud.originalQuery.GetTagProjection())
	if len(projectionTags) > 0 {
		var err error
		projTagsRefs, err := s.CreateTagRef(projectionTags...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projTagsRefs...)
	}
	projectionFields := make([]*logical.Field, len(ud.originalQuery.GetFieldProjection().GetNames()))
	for i, fieldNameProj := range ud.originalQuery.GetFieldProjection().GetNames() {
		projectionFields[i] = logical.NewField(fieldNameProj)
	}
	if len(projectionFields) > 0 {
		var err error
		projFieldRefs, err := s.CreateFieldRef(projectionFields...)
		if err != nil {
			return nil, err
		}
		s = s.ProjFields(projFieldRefs...)
	}
	limit := ud.originalQuery.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	temp := &measurev1.QueryRequest{
		TagProjection:   ud.originalQuery.TagProjection,
		FieldProjection: ud.originalQuery.FieldProjection,
		Name:            ud.originalQuery.Name,
		Groups:          ud.originalQuery.Groups,
		Criteria:        ud.originalQuery.Criteria,
		Limit:           limit + ud.originalQuery.Offset,
		OrderBy:         ud.originalQuery.OrderBy,
	}
	if ud.pushDownAgg {
		temp.GroupBy = ud.originalQuery.GroupBy
		temp.Agg = ud.originalQuery.Agg
	}
	// Prepare groupBy tags refs if needed for deduplication
	var groupByTagsRefs [][]*logical.TagRef
	if ud.pushDownAgg && ud.originalQuery.GetGroupBy() != nil {
		groupByTags := logical.ToTags(ud.originalQuery.GetGroupBy().GetTagProjection())
		var err error
		groupByTagsRefs, err = s.CreateTagRef(groupByTags...)
		if err != nil {
			return nil, err
		}
	}

	if ud.groupByEntity {
		e := s.EntityList()[0]
		sortTagSpec := s.FindTagSpecByName(e)
		if sortTagSpec == nil {
			return nil, fmt.Errorf("entity tag %s not found", e)
		}
		result := &distributedPlan{
			queryTemplate:   temp,
			s:               s,
			sortByTime:      false,
			sortTagSpec:     *sortTagSpec,
			pushDownAgg:     ud.pushDownAgg,
			groupByTagsRefs: groupByTagsRefs,
			indexMode:       indexMode,
		}
		if ud.originalQuery.OrderBy != nil && ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
	}
	if ud.originalQuery.OrderBy == nil {
		return &distributedPlan{
			queryTemplate:   temp,
			s:               s,
			sortByTime:      true,
			pushDownAgg:     ud.pushDownAgg,
			groupByTagsRefs: groupByTagsRefs,
			indexMode:       indexMode,
		}, nil
	}
	if ud.originalQuery.OrderBy.IndexRuleName == "" {
		result := &distributedPlan{
			queryTemplate:   temp,
			s:               s,
			sortByTime:      true,
			pushDownAgg:     ud.pushDownAgg,
			groupByTagsRefs: groupByTagsRefs,
			indexMode:       indexMode,
		}
		if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
	}
	ok, indexRule := s.IndexRuleDefined(ud.originalQuery.OrderBy.IndexRuleName)
	if !ok {
		return nil, fmt.Errorf("index rule %s not found", ud.originalQuery.OrderBy.IndexRuleName)
	}
	if len(indexRule.Tags) != 1 {
		return nil, fmt.Errorf("index rule %s should have only one tag", ud.originalQuery.OrderBy.IndexRuleName)
	}
	sortTagSpec := s.FindTagSpecByName(indexRule.Tags[0])
	if sortTagSpec == nil {
		return nil, fmt.Errorf("tag %s not found", indexRule.Tags[0])
	}
	result := &distributedPlan{
		queryTemplate:   temp,
		s:               s,
		sortByTime:      false,
		sortTagSpec:     *sortTagSpec,
		pushDownAgg:     ud.pushDownAgg,
		groupByTagsRefs: groupByTagsRefs,
		indexMode:       indexMode,
	}
	if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		result.desc = true
	}
	return result, nil
}

type distributedPlan struct {
	s                 logical.Schema
	queryTemplate     *measurev1.QueryRequest
	sortTagSpec       logical.TagSpec
	groupByTagsRefs   [][]*logical.TagRef
	maxDataPointsSize uint32
	sortByTime        bool
	desc              bool
	pushDownAgg       bool
	indexMode         bool
}

func (t *distributedPlan) Execute(ctx context.Context) (mi executor.MIterator, err error) {
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(t.queryTemplate).(*measurev1.QueryRequest)
	queryRequest.TimeRange = dctx.TimeRange()
	if t.maxDataPointsSize > 0 {
		queryRequest.Limit = t.maxDataPointsSize
	}
	tracer := query.GetTracer(ctx)
	var span *query.Span
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "distributed-client")
		queryRequest.Trace = true
		span.Tag("request", convert.BytesToString(logger.Proto(queryRequest)))
		span.Tag("node_selectors", fmt.Sprintf("%v", dctx.NodeSelectors()))
		span.Tag("time_range", dctx.TimeRange().String())
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				span.Stop()
			}
		}()
	}
	internalRequest := &measurev1.InternalQueryRequest{Request: queryRequest, AggReturnPartial: t.pushDownAgg}
	ff, broadcastErr := dctx.Broadcast(defaultQueryTimeout, data.TopicInternalMeasureQuery,
		bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), internalRequest))
	if broadcastErr != nil {
		return nil, broadcastErr
	}
	var see []sort.Iterator[*comparableDataPoint]
	var pushedDownAggDps []*measurev1.InternalDataPoint
	// rawFrames collects vec raw frame bodies that arrive under flag-on
	// (TopicInternalMeasureQuery's per-process wire mode is RawFrameCodec
	// and pub.go returns []byte from m.Data()). The loop accumulates all
	// non-empty bodies; nil/empty bodies are the codec's legitimate empty
	// distributed result and are skipped (matches the row-path proto path
	// where an empty InternalQueryResponse adds zero rows).
	var rawFrames [][]byte
	var responseCount int
	var dataPointCount int
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			err = multierr.Append(err, getErr)
		} else {
			switch d := m.Data().(type) {
			case *measurev1.InternalQueryResponse:
				responseCount++
				if span != nil {
					span.AddSubTrace(d.Trace)
				}
				if t.pushDownAgg {
					pushedDownAggDps = append(pushedDownAggDps, d.DataPoints...)
					dataPointCount += len(d.DataPoints)
					continue
				}
				dataPointCount += len(d.DataPoints)
				see = append(see,
					newSortableElements(d.DataPoints,
						t.sortByTime, t.sortTagSpec))
			case []byte:
				// G9f.5.c flag-on path: vec raw frame body. Empty body =
				// legitimate empty result (codec carve-out). The reduce /
				// decode happens after the loop so we can run a single
				// (shard, group)-deduped Reduce + optional Top across all
				// frames at once, matching the row path's batched dedup.
				responseCount++
				if len(d) > 0 {
					rawFrames = append(rawFrames, d)
				}
			case *common.Error:
				err = multierr.Append(err, fmt.Errorf("data node error: %s", d.Error()))
			}
		}
	}
	if span != nil {
		span.Tagf("response_count", "%d", responseCount)
		span.Tagf("data_point_count", "%d", dataPointCount)
	}
	if len(rawFrames) > 0 {
		if t.pushDownAgg {
			idps, vecErr := t.consumeRawFramesForAgg(rawFrames)
			if vecErr != nil {
				return nil, multierr.Append(err, vecErr)
			}
			// Frames already carry per-(shard,group) partials that
			// ReduceFramesToInternalDataPoints has (shard,group)-deduped
			// + Combine-d + (optionally) Top-N'd inside vec. No further
			// dedup is needed here.
			return &pushedDownAggregatedIterator{dataPoints: idps}, err
		}
		// Non-agg vec path: decode per-source so the sortedMIterator's
		// cross-iterator merge + (sid, timestamp) dedup machinery sees
		// the same per-data-node grouping the row path's proto branch
		// builds. Flattening the rows into a single sortableElements
		// defeats the dedup because duplicate rows from different sources
		// land in different sortField groups across the flat sequence.
		perSource, vecErr := vmeasure.DecodeFramesPerSource(rawFrames)
		if vecErr != nil {
			return nil, multierr.Append(err, vecErr)
		}
		for _, idps := range perSource {
			see = append(see, newSortableElements(idps, t.sortByTime, t.sortTagSpec))
		}
	}
	if t.pushDownAgg {
		deduplicatedDps, dedupErr := deduplicateAggregatedDataPointsWithShard(pushedDownAggDps, t.groupByTagsRefs)
		if dedupErr != nil {
			return nil, multierr.Append(err, dedupErr)
		}
		return &pushedDownAggregatedIterator{dataPoints: deduplicatedDps}, err
	}
	smi := &sortedMIterator{
		Iterator:  sort.NewItemIter(see, t.desc),
		indexMode: t.indexMode,
	}
	smi.init()
	return smi, err
}

// consumeRawFramesForAgg decodes the per-data-node vec raw frame bodies
// for the pushDownAgg path into a flat []*measurev1.InternalDataPoint:
// the frames carry per-(shard, group) AggModeMap partials and the
// pipeline runs ReduceFramesToInternalDataPoints (frame.Decode →
// AggModeReduce with (shard, group) replica dedup → optional liaison-
// side BatchTop → reverse-serialise to proto datapoints).
//
// The non-agg path uses vmeasure.DecodeFramesPerSource directly so the
// per-data-node grouping survives the round-trip and the row-side
// sortedMIterator's cross-iterator merge + (sid, timestamp) dedup keeps
// working unchanged.
func (t *distributedPlan) consumeRawFramesForAgg(frames [][]byte) ([]*measurev1.InternalDataPoint, error) {
	keyTagNames := t.rawFrameKeyTagNames()
	aggSpecs, aggErr := t.rawFrameAggSpecs()
	if aggErr != nil {
		return nil, aggErr
	}
	topSpec := t.rawFrameTopSpec()
	// Liaison-side tracker matches the row path's batch sizes; vec uses a
	// fixed 1024-row batch under ReduceRawFrames so each reduced batch
	// stays bounded. A large NewMemoryTracker keeps the reduce free of
	// budget rejections — the row path's distributed reduce is similarly
	// unbounded.
	tracker := vectorized.NewMemoryTracker(1 << 30)
	return vmeasure.ReduceFramesToInternalDataPoints(frames, keyTagNames, aggSpecs, topSpec, 1024, tracker)
}

// rawFrameKeyTagNames returns the GroupBy tag names from the
// queryTemplate, in projection order. Empty result means scalar reduce
// (no group key) which Reduce handles as a single output row.
func (t *distributedPlan) rawFrameKeyTagNames() []string {
	gb := t.queryTemplate.GetGroupBy()
	if gb == nil {
		return nil
	}
	tp := gb.GetTagProjection()
	if tp == nil {
		return nil
	}
	var names []string
	for _, fam := range tp.GetTagFamilies() {
		names = append(names, fam.GetTags()...)
	}
	return names
}

// rawFrameAggSpecs builds the per-agg AggReduceSpec list from the
// queryTemplate.Agg. v1 supports a single agg; OutputName mirrors the
// data-node side's AggSpec.Output (the Agg.FieldName, matching
// vmeasure.plan.BuildOperators).
func (t *distributedPlan) rawFrameAggSpecs() ([]vmeasure.AggReduceSpec, error) {
	agg := t.queryTemplate.GetAgg()
	if agg == nil {
		return nil, nil
	}
	fn, mapErr := rawFrameAggFunc(agg.GetFunction())
	if mapErr != nil {
		return nil, mapErr
	}
	return []vmeasure.AggReduceSpec{{
		OutputName: agg.GetFieldName(),
		Func:       fn,
	}}, nil
}

// rawFrameTopSpec returns an optional Top spec when the queryTemplate
// carries a Top clause. asc/desc mirrors the analyzer's mapping
// (modelv1.Sort_SORT_ASC → asc=true, anything else → desc).
func (t *distributedPlan) rawFrameTopSpec() *vmeasure.ReduceTopSpec {
	top := t.queryTemplate.GetTop()
	if top == nil {
		return nil
	}
	asc := top.GetFieldValueSort() == modelv1.Sort_SORT_ASC
	return &vmeasure.ReduceTopSpec{
		FieldName: top.GetFieldName(),
		N:         int(top.GetNumber()),
		Asc:       asc,
	}
}

// rawFrameAggFunc translates the proto-level AggregationFunction to the
// vec-level AggFunc consumed by the liaison Reduce. Mirrors
// vmeasure.toModelAggFunc in reverse.
func rawFrameAggFunc(af modelv1.AggregationFunction) (vmeasure.AggFunc, error) {
	switch af {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return vmeasure.AggSum, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return vmeasure.AggCount, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return vmeasure.AggMin, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return vmeasure.AggMax, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return vmeasure.AggMean, nil
	}
	return 0, fmt.Errorf("measure: unknown AggregationFunction %v for vec reduce", af)
}

func (t *distributedPlan) String() string {
	return fmt.Sprintf("distributed:%s", t.queryTemplate.String())
}

func (t *distributedPlan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (t *distributedPlan) Schema() logical.Schema {
	if t.pushDownAgg {
		return &pushDownAggSchema{
			originalSchema:   t.s,
			aggregationField: logical.NewField(t.queryTemplate.Agg.FieldName),
		}
	}
	return t.s
}

func (t *distributedPlan) Limit(maxVal int) {
	t.maxDataPointsSize = uint32(maxVal)
}

var _ sort.Comparable = (*comparableDataPoint)(nil)

type comparableDataPoint struct {
	*measurev1.InternalDataPoint
	sortField []byte
}

func newComparableElement(idp *measurev1.InternalDataPoint, sortByTime bool, sortTagSpec logical.TagSpec) (*comparableDataPoint, error) {
	dp := idp.GetDataPoint()
	var sortField []byte
	if sortByTime {
		sortField = convert.Uint64ToBytes(uint64(dp.Timestamp.AsTime().UnixNano()))
	} else {
		var err error
		sortField, err = pbv1.MarshalTagValue(dp.TagFamilies[sortTagSpec.TagFamilyIdx].Tags[sortTagSpec.TagIdx].Value)
		if err != nil {
			return nil, err
		}
	}

	return &comparableDataPoint{
		InternalDataPoint: idp,
		sortField:         sortField,
	}, nil
}

func (e *comparableDataPoint) SortedField() []byte {
	return e.sortField
}

var _ sort.Iterator[*comparableDataPoint] = (*sortableElements)(nil)

type sortableElements struct {
	cur          *comparableDataPoint
	dataPoints   []*measurev1.InternalDataPoint
	sortTagSpec  logical.TagSpec
	index        int
	isSortByTime bool
}

func newSortableElements(dataPoints []*measurev1.InternalDataPoint, isSortByTime bool, sortTagSpec logical.TagSpec) *sortableElements {
	return &sortableElements{
		dataPoints:   dataPoints,
		isSortByTime: isSortByTime,
		sortTagSpec:  sortTagSpec,
	}
}

func (*sortableElements) Close() error {
	return nil
}

func (s *sortableElements) Next() bool {
	return s.iter(func(idp *measurev1.InternalDataPoint) (*comparableDataPoint, error) {
		return newComparableElement(idp, s.isSortByTime, s.sortTagSpec)
	})
}

func (s *sortableElements) Val() *comparableDataPoint {
	return s.cur
}

func (s *sortableElements) iter(fn func(*measurev1.InternalDataPoint) (*comparableDataPoint, error)) bool {
	if s.index >= len(s.dataPoints) {
		return false
	}
	cur, err := fn(s.dataPoints[s.index])
	s.index++
	if err != nil {
		return s.iter(fn)
	}
	s.cur = cur
	return s.index <= len(s.dataPoints)
}

var _ executor.MIterator = (*sortedMIterator)(nil)

type sortedMIterator struct {
	sort.Iterator[*comparableDataPoint]
	data        *list.List
	uniqueData  map[uint64]*measurev1.InternalDataPoint
	cur         *measurev1.InternalDataPoint
	seenSids    map[uint64]struct{}
	initialized bool
	exhausted   bool
	closed      bool
	indexMode   bool
}

func (s *sortedMIterator) init() {
	if s.initialized {
		return
	}
	s.initialized = true
	if !s.Iterator.Next() {
		s.exhausted = true
		return
	}
	s.data = list.New()
	s.uniqueData = make(map[uint64]*measurev1.InternalDataPoint)
	if s.indexMode {
		s.seenSids = make(map[uint64]struct{})
	}
	s.loadDps()
}

func (s *sortedMIterator) Next() bool {
	if s.data == nil {
		return false
	}
	if s.data.Len() == 0 {
		s.loadDps()
		if s.data.Len() == 0 {
			return false
		}
	}
	idp := s.data.Front()
	s.data.Remove(idp)
	s.cur = idp.Value.(*measurev1.InternalDataPoint)
	return true
}

// loadDps consumes sort-field-equal groups from the underlying iterator and
// pushes their deduplicated rows into s.data. When indexMode is true, rows
// whose Sid was already emitted in a prior group are skipped — without this,
// cross-node duplicates that carry different per-node "last-write" timestamps
// would slip past hashDataPoint (which keys on Sid+timestamp) because they
// land in different sort-field groups. When every row in a group is filtered,
// the loop advances to the next group so callers see continued iteration.
func (s *sortedMIterator) loadDps() {
	for !s.exhausted {
		s.loadOneGroup()
		if s.data.Len() > 0 {
			return
		}
	}
}

func (s *sortedMIterator) loadOneGroup() {
	for k := range s.uniqueData {
		delete(s.uniqueData, k)
	}
	first := s.Iterator.Val()
	s.uniqueData[hashDataPoint(first.GetDataPoint())] = first.InternalDataPoint
	for {
		if !s.Iterator.Next() {
			s.exhausted = true
			break
		}
		v := s.Iterator.Val()
		if bytes.Equal(first.SortedField(), v.SortedField()) {
			key := hashDataPoint(v.GetDataPoint())
			if existed, ok := s.uniqueData[key]; ok {
				if v.GetDataPoint().Version > existed.GetDataPoint().Version {
					s.uniqueData[key] = v.InternalDataPoint
				}
			} else {
				s.uniqueData[key] = v.InternalDataPoint
			}
		} else {
			break
		}
	}
	for _, v := range s.uniqueData {
		if s.indexMode {
			sid := v.GetDataPoint().GetSid()
			if _, dup := s.seenSids[sid]; dup {
				continue
			}
			s.seenSids[sid] = struct{}{}
		}
		s.data.PushBack(v)
	}
}

func (s *sortedMIterator) Current() []*measurev1.InternalDataPoint {
	return []*measurev1.InternalDataPoint{s.cur}
}

func (s *sortedMIterator) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.exhausted = true
	s.data = nil
	s.uniqueData = nil
	s.seenSids = nil
	s.cur = nil
	if s.Iterator == nil {
		return nil
	}
	return s.Iterator.Close()
}

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// hashDataPoint calculates the hash value of a data point with fnv64a.
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func hashDataPoint(dp *measurev1.DataPoint) uint64 {
	h := uint64(offset64)
	h = (h ^ dp.Sid) * prime64
	h = (h ^ uint64(dp.Timestamp.Seconds)) * prime64
	h = (h ^ uint64(dp.Timestamp.Nanos)) * prime64
	return h
}

type pushedDownAggregatedIterator struct {
	dataPoints []*measurev1.InternalDataPoint
	index      int
}

func (s *pushedDownAggregatedIterator) Next() bool {
	if s.index >= len(s.dataPoints) {
		return false
	}
	s.index++
	return true
}

func (s *pushedDownAggregatedIterator) Current() []*measurev1.InternalDataPoint {
	if s.index == 0 || s.index > len(s.dataPoints) {
		return nil
	}
	return []*measurev1.InternalDataPoint{s.dataPoints[s.index-1]}
}

func (s *pushedDownAggregatedIterator) Close() error {
	return nil
}

// deduplicateAggregatedDataPointsWithShard removes duplicate aggregated results from multiple replicas
// of the same shard, while preserving results from different shards.
func deduplicateAggregatedDataPointsWithShard(dataPoints []*measurev1.InternalDataPoint, groupByTagsRefs [][]*logical.TagRef) ([]*measurev1.InternalDataPoint, error) {
	if len(groupByTagsRefs) == 0 {
		// No group-by: deduplicate by shard_id only
		seen := make(map[uint32]struct{})
		result := make([]*measurev1.InternalDataPoint, 0, len(dataPoints))
		for _, idp := range dataPoints {
			if _, exists := seen[idp.GetShardId()]; !exists {
				seen[idp.GetShardId()] = struct{}{}
				result = append(result, idp)
			}
		}
		return result, nil
	}
	// key = hash(shard_id, group_key)
	// Same shard with same group key will be deduplicated
	// Different shards with same group key will be preserved
	groupMap := make(map[uint64]struct{})
	result := make([]*measurev1.InternalDataPoint, 0, len(dataPoints))
	for _, idp := range dataPoints {
		groupKey, keyErr := formatGroupByKey(idp.DataPoint, groupByTagsRefs)
		if keyErr != nil {
			return nil, keyErr
		}
		key := hashWithShard(uint64(idp.ShardId), groupKey)
		if _, exists := groupMap[key]; !exists {
			groupMap[key] = struct{}{}
			result = append(result, idp)
		}
	}
	return result, nil
}

// hashWithShard combines shard_id and group_key into a single hash.
func hashWithShard(shardID, groupKey uint64) uint64 {
	h := uint64(offset64)
	h = (h ^ shardID) * prime64
	h = (h ^ groupKey) * prime64
	return h
}
