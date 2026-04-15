package main

import (
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

// TestCase represents a generated test case with all its artifacts.
type TestCase struct {
	Name      string
	Measure   *Measure
	Request   *measurev1.QueryRequest
	QL        string // rendered QL query string
	WantErr   bool
	WantEmpty bool
	DisOrder  bool
	Duration  string // Go duration expression for helpers.Args
}

// WriteQLFile writes the .ql file content.
func (tc *TestCase) QLFileContent() []byte {
	return []byte(tc.QL + "\n")
}

// WriteInputYAML writes the input YAML file content.
func (tc *TestCase) InputYAMLContent() ([]byte, error) {
	marshaler := protojson.MarshalOptions{Multiline: true}
	jsonBytes, marshalErr := marshaler.Marshal(tc.Request)
	if marshalErr != nil {
		return nil, marshalErr
	}
	return yaml.JSONToYAML(jsonBytes)
}

// BuildTagProjection creates a TagProjection for the given tag names.
func BuildTagProjection(m *Measure, tagNames []string) *modelv1.TagProjection {
	families := make(map[string][]string)
	for _, tagName := range tagNames {
		for _, tag := range m.Tags {
			if tag.Name == tagName {
				families[tag.Family] = append(families[tag.Family], tagName)
				break
			}
		}
	}
	var tagFamilies []*modelv1.TagProjection_TagFamily
	for familyName, tags := range families {
		tagFamilies = append(tagFamilies, &modelv1.TagProjection_TagFamily{
			Name: familyName,
			Tags: tags,
		})
	}
	return &modelv1.TagProjection{TagFamilies: tagFamilies}
}

// BuildFieldProjection creates a FieldProjection for the given field names.
// Returns nil when fieldNames is empty so the proto marshals without the fieldProjection key.
func BuildFieldProjection(fieldNames []string) *measurev1.QueryRequest_FieldProjection {
	if len(fieldNames) == 0 {
		return nil
	}
	return &measurev1.QueryRequest_FieldProjection{Names: fieldNames}
}

// BuildCondition creates a Condition proto.
func BuildCondition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Condition {
	return &modelv1.Condition{
		Name:  name,
		Op:    op,
		Value: value,
	}
}

// BuildCriteriaFromCondition wraps a Condition in a Criteria.
func BuildCriteriaFromCondition(cond *modelv1.Condition) *modelv1.Criteria {
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{Condition: cond},
	}
}

// BuildLogicalExpr creates a LogicalExpression Criteria.
func BuildLogicalExpr(op modelv1.LogicalExpression_LogicalOp, left, right *modelv1.Criteria) *modelv1.Criteria {
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Le{
			Le: &modelv1.LogicalExpression{
				Op:    op,
				Left:  left,
				Right: right,
			},
		},
	}
}

// TagValueStr creates a string TagValue.
func TagValueStr(val string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: val}}}
}

// TagValueInt creates an int TagValue.
func TagValueInt(val int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: val}}}
}

// TagValueStrArray creates a string array TagValue.
func TagValueStrArray(vals []string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: vals}}}
}

// TagValueIntArray creates an int array TagValue.
func TagValueIntArray(vals []int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: vals}}}
}

// TagValueNull creates a null TagValue.
func TagValueNull() *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
}

// AllTagNames returns all tag names for a measure.
func AllTagNames(m *Measure) []string {
	names := make([]string, 0, len(m.Tags))
	for _, tag := range m.Tags {
		names = append(names, tag.Name)
	}
	return names
}

// AllFieldNames returns all field names for a measure.
func AllFieldNames(m *Measure) []string {
	names := make([]string, 0, len(m.Fields))
	for _, field := range m.Fields {
		names = append(names, field.Name)
	}
	return names
}
