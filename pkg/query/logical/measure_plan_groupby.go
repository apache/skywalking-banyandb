package logical

import (
	"fmt"
	"hash/fnv"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

var (
	_ UnresolvedPlan      = (*unresolvedGroupByAggregation)(nil)
	_ Plan                = (*groupByAggregation)(nil)
	_ AggregationOperator = (*integerAggregation)(nil)
)

type unresolvedGroupByAggregation struct {
	unresolvedInput *unresolvedMeasureIndexScan
	// aggrFunc is the type of aggregation
	aggrFunc modelv1.AggregationFunction
	// groupByTags should be a subset of tag projection
	groupByTags      [][]*Tag
	aggregationField *Field
}

func (gba *unresolvedGroupByAggregation) Analyze(measureSchema Schema) (Plan, error) {
	indexScanPlan, err := gba.unresolvedInput.Analyze(measureSchema)
	if err != nil {
		return nil, err
	}
	// check validity of groupBy tags
	groupByTagRefs, err := indexScanPlan.Schema().CreateTagRef(gba.groupByTags...)
	if err != nil {
		return nil, err
	}
	// check validity of aggregation fields
	aggregationFieldRefs, err := indexScanPlan.Schema().CreateFieldRef(gba.aggregationField)
	if err != nil {
		return nil, err
	}
	if len(aggregationFieldRefs) == 0 {
		return nil, errors.Wrap(ErrFieldNotDefined, "aggregation schema")
	}
	return &groupByAggregation{
		parent: &parent{
			unresolvedInput: gba.unresolvedInput,
			input:           indexScanPlan,
		},
		schema:              measureSchema,
		groupByTagsRefs:     groupByTagRefs,
		aggregationFieldRef: aggregationFieldRefs[0],
	}, nil
}

type groupByAggregation struct {
	*parent
	schema              Schema
	groupByTagsRefs     [][]*TagRef
	aggregationFieldRef *FieldRef
	aggregationType     modelv1.AggregationFunction
}

func (g *groupByAggregation) String() string {
	return fmt.Sprintf("GroupByAggregation: aggreation{type=%d,field=%s}; groupBy=%s",
		g.aggregationType,
		g.aggregationFieldRef.field.name,
		formatTagRefs(", ", g.groupByTagsRefs...))
}

func (g *groupByAggregation) Type() PlanType {
	return PlanGroupByAggregation
}

func (g *groupByAggregation) Equal(plan Plan) bool {
	if plan.Type() != PlanLocalIndexScan {
		return false
	}
	other := plan.(*groupByAggregation)
	if g.aggregationType == other.aggregationType &&
		cmp.Equal(g.groupByTagsRefs, other.groupByTagsRefs) &&
		cmp.Equal(g.aggregationFieldRef, other.aggregationFieldRef) {
		return g.parent.input.Equal(other.parent.input)
	}

	return false
}

func (g *groupByAggregation) Children() []Plan {
	return []Plan{g.input}
}

func (g *groupByAggregation) Schema() Schema {
	return g.schema.ProjField(g.aggregationFieldRef).ProjTags(g.groupByTagsRefs...)
}

func (g *groupByAggregation) Execute(ec executor.MeasureExecutionContext) ([]*measurev1.DataPoint, error) {
	aggregationMap := make(map[uint64]AggregationOperator)
	dataPoints, err := g.parent.input.(executor.MeasureExecutable).Execute(ec)
	if err != nil {
		return nil, err
	}
	for _, dp := range dataPoints {
		key, innerErr := g.formatGroupByKey(dp)
		if innerErr != nil {
			return nil, innerErr
		}
		op, ok := aggregationMap[key]
		if !ok {
			op = g.createAggregationOp()
		}
		if op == nil {
			return nil, errors.New("aggregation op does not exist")
		}
		op.Update(dp)
	}
	result := make([]*measurev1.DataPoint, 0, len(aggregationMap))
	for _, op := range aggregationMap {
		result = append(result, op.Snapshot(g.aggregationFieldRef, g.groupByTagsRefs))
	}
	return result, nil
}

func (g *groupByAggregation) createAggregationOp() AggregationOperator {
	switch g.aggregationFieldRef.DataType() {
	case int32(databasev1.FieldType_FIELD_TYPE_INT):
		return NewIntAggregation(g.aggregationType, g.aggregationFieldRef)
	default:
		return nil
	}
}

func (g *groupByAggregation) formatGroupByKey(point *measurev1.DataPoint) (uint64, error) {
	hash := fnv.New64a()
	for _, tagFamilyRef := range g.groupByTagsRefs {
		for _, tagRef := range tagFamilyRef {
			tag := point.GetTagFamilies()[tagRef.Spec.TagFamilyIdx].GetTags()[tagRef.Spec.TagIdx]
			switch v := tag.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str:
				_, innerErr := hash.Write([]byte(v.Str.GetValue()))
				if innerErr != nil {
					return 0, innerErr
				}
			case *modelv1.TagValue_Int:
				_, innerErr := hash.Write(convert.Int64ToBytes(v.Int.GetValue()))
				if innerErr != nil {
					return 0, innerErr
				}
			case *modelv1.TagValue_IntArray, *modelv1.TagValue_StrArray, *modelv1.TagValue_BinaryData:
				return 0, errors.New("group-by on array/binary tag is not supported")
			}
		}
	}
	return hash.Sum64(), nil
}

type AggregationOperator interface {
	Update(*measurev1.DataPoint)
	Snapshot(*FieldRef, [][]*TagRef) *measurev1.DataPoint
}

type integerAggregation struct {
	fieldRef        *FieldRef
	aggregationFunc func(val int64, point *measurev1.DataPoint) int64
	snapshotFunc    func(counter, val int64) int64
	snapshot        *measurev1.DataPoint
	counter         int64
	val             int64
}

func (i *integerAggregation) Snapshot(fieldRef *FieldRef, tagRefs [][]*TagRef) *measurev1.DataPoint {
	newDataPoint := new(measurev1.DataPoint)
	newDataPoint.Fields = []*measurev1.DataPoint_Field{
		{
			Name: fieldRef.field.name,
			Value: &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{
						Value: i.snapshotFunc(i.counter, i.val),
					},
				},
			},
		},
	}
	newDataPoint.Timestamp = i.snapshot.Timestamp
	newDataPoint.TagFamilies = make([]*modelv1.TagFamily, len(tagRefs))
	for idx, tagFamilyRef := range tagRefs {
		newDataPoint.TagFamilies[idx] = &modelv1.TagFamily{
			Name: tagFamilyRef[0].tag.GetFamilyName(),
			Tags: make([]*modelv1.Tag, len(tagFamilyRef)),
		}
		for jdx, tagRef := range tagFamilyRef {
			newDataPoint.TagFamilies[idx].Tags[jdx] = i.snapshot.GetTagFamilies()[tagRef.Spec.TagFamilyIdx].GetTags()[tagRef.Spec.TagIdx]
		}
	}
	return newDataPoint
}

func (i *integerAggregation) Update(point *measurev1.DataPoint) {
	i.val = i.aggregationFunc(i.val, point)
	i.counter += 1
	i.snapshot = point
}

func NewIntAggregation(aggrType modelv1.AggregationFunction, ref *FieldRef) AggregationOperator {
	switch aggrType {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return &integerAggregation{
			snapshotFunc: func(counter, _val int64) int64 {
				return counter
			},
		}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return &integerAggregation{
			aggregationFunc: func(i int64, point *measurev1.DataPoint) int64 {
				curVal := point.GetFields()[ref.Spec.FieldIdx].GetValue().GetInt().GetValue()
				if i > curVal {
					return i
				}
				return curVal
			},
			snapshotFunc: func(counter, val int64) int64 {
				return val
			},
		}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return &integerAggregation{
			aggregationFunc: func(i int64, point *measurev1.DataPoint) int64 {
				curVal := point.GetFields()[ref.Spec.FieldIdx].GetValue().GetInt().GetValue()
				if i < curVal {
					return i
				}
				return curVal
			},
			snapshotFunc: func(counter, val int64) int64 {
				return val
			},
		}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return &integerAggregation{
			aggregationFunc: func(i int64, point *measurev1.DataPoint) int64 {
				curVal := point.GetFields()[ref.Spec.FieldIdx].GetValue().GetInt().GetValue()
				return i + curVal
			},
			snapshotFunc: func(_counter, val int64) int64 {
				return val
			},
		}
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return &integerAggregation{
			aggregationFunc: func(i int64, point *measurev1.DataPoint) int64 {
				curVal := point.GetFields()[ref.Spec.FieldIdx].GetValue().GetInt().GetValue()
				return i + curVal
			},
			snapshotFunc: func(counter, val int64) int64 {
				return val / counter
			},
		}
	}
	return nil
}
