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

package main

import (
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// TestCase represents a generated test case with all its artifacts.
type TestCase struct {
	Measure   *Measure
	Request   *measurev1.QueryRequest
	Name      string
	QL        string
	Duration  string
	WantErr   bool
	WantEmpty bool
	DisOrder  bool
}

const licenseHeader = "# Licensed to Apache Software Foundation (ASF) under one or more contributor\n" +
	"# license agreements. See the NOTICE file distributed with\n" +
	"# this work for additional information regarding copyright\n" +
	"# ownership. Apache Software Foundation (ASF) licenses this file to you under\n" +
	"# the Apache License, Version 2.0 (the \"License\"); you may\n" +
	"# not use this file except in compliance with the License.\n" +
	"# You may obtain a copy of the License at\n" +
	"#\n" +
	"#     http://www.apache.org/licenses/LICENSE-2.0\n" +
	"#\n" +
	"# Unless required by applicable law or agreed to in writing,\n" +
	"# software distributed under the License is distributed on an\n" +
	"# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n" +
	"# KIND, either express or implied.  See the License for the\n" +
	"# specific language governing permissions and limitations\n" +
	"# under the License.\n\n"

// QLFileContent returns the .ql file content with license header.
func (tc *TestCase) QLFileContent() []byte {
	return []byte(licenseHeader + tc.QL + "\n")
}

// InputYAMLContent returns the input YAML file content with license header.
func (tc *TestCase) InputYAMLContent() ([]byte, error) {
	marshaler := protojson.MarshalOptions{Multiline: true}
	jsonBytes, marshalErr := marshaler.Marshal(tc.Request)
	if marshalErr != nil {
		return nil, marshalErr
	}
	yamlBytes, yamlErr := yaml.JSONToYAML(jsonBytes)
	if yamlErr != nil {
		return nil, yamlErr
	}
	return append([]byte(licenseHeader), yamlBytes...), nil
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
