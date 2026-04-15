package main

import (
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// LeafOp defines a valid BinaryOp + TagValue + measure + tag combination.
type LeafOp struct {
	Op         modelv1.Condition_BinaryOp
	ValueType  string           // "str", "int", "null", "str_array", "int_array"
	Value      *modelv1.TagValue // example value to use
	Measure    string           // target measure name
	TagName    string           // tag to filter on
	QLValue    string           // QL representation of the value
}

// GenerateLayer1 produces test cases for the Criteria leaf layer.
// Covers 9 in-scope BinaryOps × their valid TagValue types = 18 pairs.
func GenerateLayer1() []*TestCase {
	ops := defineLeafOps()
	var cases []*TestCase
	for idx, op := range ops {
		m := FindMeasure(op.Measure)
		if m == nil {
			continue
		}
		req := buildLeafRequest(m, op)
		ql, qlErr := RenderQL(req)
		if qlErr != nil {
			continue
		}
		cases = append(cases, &TestCase{
			Name:    fmt.Sprintf("gen_leaf_%s_%s", binaryOpShortName(op.Op), op.ValueType),
			Measure: m,
			Request: req,
			QL:      ql,
			WantErr:  op.ValueType == "null", // server rejects null value comparisons
			Duration: "25 * time.Minute",
		})
		_ = idx
	}

	// Add error cases for invalid combinations
	cases = append(cases, generateLeafErrorCases()...)

	return cases
}

func defineLeafOps() []LeafOp {
	return []LeafOp{
		// EQ with str, int, null
		{Op: modelv1.Condition_BINARY_OP_EQ, ValueType: "str", Value: TagValueStr("svc1"), Measure: "service_cpm_minute", TagName: "id", QLValue: "'svc1'"},
		{Op: modelv1.Condition_BINARY_OP_EQ, ValueType: "int", Value: TagValueInt(1), Measure: "service_traffic", TagName: "layer", QLValue: "1"},
		{Op: modelv1.Condition_BINARY_OP_EQ, ValueType: "null", Value: TagValueNull(), Measure: "service_cpm_minute", TagName: "id", QLValue: "NULL"},
		// NE with str, int, null
		{Op: modelv1.Condition_BINARY_OP_NE, ValueType: "str", Value: TagValueStr("svc1"), Measure: "service_cpm_minute", TagName: "id", QLValue: "'svc1'"},
		{Op: modelv1.Condition_BINARY_OP_NE, ValueType: "int", Value: TagValueInt(1), Measure: "service_traffic", TagName: "layer", QLValue: "1"},
		{Op: modelv1.Condition_BINARY_OP_NE, ValueType: "null", Value: TagValueNull(), Measure: "service_cpm_minute", TagName: "id", QLValue: "NULL"},
		// LT with str, int
		{Op: modelv1.Condition_BINARY_OP_LT, ValueType: "str", Value: TagValueStr("svc2"), Measure: "service_cpm_minute", TagName: "id", QLValue: "'svc2'"},
		{Op: modelv1.Condition_BINARY_OP_LT, ValueType: "int", Value: TagValueInt(2), Measure: "service_traffic", TagName: "layer", QLValue: "2"},
		// GT with str, int
		{Op: modelv1.Condition_BINARY_OP_GT, ValueType: "str", Value: TagValueStr("svc1"), Measure: "service_cpm_minute", TagName: "id", QLValue: "'svc1'"},
		{Op: modelv1.Condition_BINARY_OP_GT, ValueType: "int", Value: TagValueInt(1), Measure: "service_traffic", TagName: "layer", QLValue: "1"},
		// LE with str, int
		{Op: modelv1.Condition_BINARY_OP_LE, ValueType: "str", Value: TagValueStr("svc2"), Measure: "service_cpm_minute", TagName: "id", QLValue: "'svc2'"},
		{Op: modelv1.Condition_BINARY_OP_LE, ValueType: "int", Value: TagValueInt(1), Measure: "service_traffic", TagName: "layer", QLValue: "1"},
		// GE with str, int
		{Op: modelv1.Condition_BINARY_OP_GE, ValueType: "str", Value: TagValueStr("svc2"), Measure: "service_cpm_minute", TagName: "id", QLValue: "'svc2'"},
		{Op: modelv1.Condition_BINARY_OP_GE, ValueType: "int", Value: TagValueInt(2), Measure: "service_traffic", TagName: "layer", QLValue: "2"},
		// IN with str_array, int_array
		{Op: modelv1.Condition_BINARY_OP_IN, ValueType: "str_array", Value: TagValueStrArray([]string{"svc1", "svc2"}), Measure: "service_cpm_minute", TagName: "id", QLValue: "('svc1', 'svc2')"},
		{Op: modelv1.Condition_BINARY_OP_IN, ValueType: "int_array", Value: TagValueIntArray([]int64{1, 2}), Measure: "service_traffic", TagName: "layer", QLValue: "(1, 2)"},
		// NOT_IN with str_array, int_array
		{Op: modelv1.Condition_BINARY_OP_NOT_IN, ValueType: "str_array", Value: TagValueStrArray([]string{"svc3"}), Measure: "service_cpm_minute", TagName: "id", QLValue: "('svc3')"},
		{Op: modelv1.Condition_BINARY_OP_NOT_IN, ValueType: "int_array", Value: TagValueIntArray([]int64{0}), Measure: "service_traffic", TagName: "layer", QLValue: "(0)"},
		// MATCH with str
		{Op: modelv1.Condition_BINARY_OP_MATCH, ValueType: "str", Value: TagValueStr("nodea"), Measure: "service_instance_traffic", TagName: "name", QLValue: "MATCH('nodea')"},
	}
}

func buildLeafRequest(m *Measure, op LeafOp) *measurev1.QueryRequest {
	cond := BuildCondition(op.TagName, op.Op, op.Value)
	criteria := BuildCriteriaFromCondition(cond)
	tagNames := AllTagNames(m)
	fieldNames := AllFieldNames(m)
	return &measurev1.QueryRequest{
		Name:           m.Name,
		Groups:         []string{m.Group},
		Criteria:       criteria,
		TagProjection:  BuildTagProjection(m, tagNames),
		FieldProjection: BuildFieldProjection(fieldNames),
	}
}

func generateLeafErrorCases() []*TestCase {
	return []*TestCase{
		// IN with scalar (should use array)
		{
			Name: "gen_err_in_scalar",
			Measure: FindMeasure("service_cpm_minute"),
			Request: &measurev1.QueryRequest{
				Name:   "service_cpm_minute",
				Groups: []string{"sw_metric"},
				Criteria: BuildCriteriaFromCondition(
					BuildCondition("id", modelv1.Condition_BINARY_OP_IN, TagValueStr("svc1")),
				),
				TagProjection:  BuildTagProjection(FindMeasure("service_cpm_minute"), []string{"id", "entity_id"}),
				FieldProjection: BuildFieldProjection([]string{"total", "value"}),
			},
			QL:      "SELECT id, entity_id, total, value FROM MEASURE service_cpm_minute IN sw_metric TIME > '-15m' WHERE id IN ('svc1')",
			WantErr: true,
		},
		// MATCH with int value
		{
			Name: "gen_err_match_int",
			Measure: FindMeasure("service_traffic"),
			Request: &measurev1.QueryRequest{
				Name:   "service_traffic",
				Groups: []string{"index_mode"},
				Criteria: BuildCriteriaFromCondition(
					BuildCondition("layer", modelv1.Condition_BINARY_OP_MATCH, TagValueInt(1)),
				),
				TagProjection: BuildTagProjection(FindMeasure("service_traffic"), []string{"id", "service_id", "name", "short_name", "service_group", "layer"}),
			},
			QL:      "SELECT id, service_id, name, short_name, service_group, layer FROM MEASURE service_traffic IN index_mode TIME > '-15m' WHERE layer MATCH 1",
			WantErr: true,
		},
	}
}

func binaryOpShortName(op modelv1.Condition_BinaryOp) string {
	switch op {
	case modelv1.Condition_BINARY_OP_EQ:
		return "eq"
	case modelv1.Condition_BINARY_OP_NE:
		return "ne"
	case modelv1.Condition_BINARY_OP_LT:
		return "lt"
	case modelv1.Condition_BINARY_OP_GT:
		return "gt"
	case modelv1.Condition_BINARY_OP_LE:
		return "le"
	case modelv1.Condition_BINARY_OP_GE:
		return "ge"
	case modelv1.Condition_BINARY_OP_IN:
		return "in"
	case modelv1.Condition_BINARY_OP_NOT_IN:
		return "not_in"
	case modelv1.Condition_BINARY_OP_MATCH:
		return "match"
	case modelv1.Condition_BINARY_OP_HAVING:
		return "having"
	case modelv1.Condition_BINARY_OP_NOT_HAVING:
		return "not_having"
	default:
		return "unknown"
	}
}
