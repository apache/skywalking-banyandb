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
	"fmt"
	"slices"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// MergeTopNResults combines two stored field values of one data point of the Top-N result
// measure the way a query and a part merge do. entity holds the data point's entity values
// in the order of TopNTagNames.
func MergeTopNResults(entity []*modelv1.TagValue, timestamp, leftVersion, rightVersion int64, left, right []byte) ([]byte, error) {
	if err := checkTopNEntity(entity); err != nil {
		return nil, err
	}
	// Merging a value with an identical copy of itself (a replayed import) yields that value;
	// returning it as is avoids a decode/encode round trip that can rewrite empty entity values.
	if bytes.Equal(left, right) {
		return left, nil
	}
	params, err := ParseTopNParameters(entity[3].GetStr().GetValue())
	if err != nil {
		return nil, err
	}
	decoder := GenerateTopNValuesDecoder()
	defer ReleaseTopNValuesDecoder(decoder)
	return MergeTopNBinaryValues(left, right, int32(params.Limit), modelv1.Sort(entity[1].GetInt().GetValue()), decoder,
		uint64(timestamp), leftVersion, rightVersion, params.FieldType)
}

// RenderTopNResult renders a stored field value of one data point of the Top-N result
// measure for comparison: its value name, entity tag names and items, the items sorted, so
// that equal results render equally whatever the order of their tied items. entity holds the
// data point's entity values in the order of TopNTagNames.
func RenderTopNResult(entity []*modelv1.TagValue, value []byte) (string, error) {
	if err := checkTopNEntity(entity); err != nil {
		return "", err
	}
	params, err := ParseTopNParameters(entity[3].GetStr().GetValue())
	if err != nil {
		return "", err
	}
	if params.FieldType == databasev1.FieldType_FIELD_TYPE_FLOAT {
		return renderTopNValue[float64](value)
	}
	return renderTopNValue[int64](value)
}

func checkTopNEntity(entity []*modelv1.TagValue) error {
	if len(entity) != len(TopNTagNames) {
		return fmt.Errorf("a %s data point has %d entity values, want %d", TopNSchemaName, len(entity), len(TopNTagNames))
	}
	return nil
}

func renderTopNValue[K int64 | float64](value []byte) (string, error) {
	v := &TopNValue[K]{}
	decoder := GenerateTopNValuesDecoder()
	defer ReleaseTopNValuesDecoder(decoder)
	if err := v.Unmarshal(value, decoder); err != nil {
		return "", err
	}
	name, tags, values, entities := v.Values()
	items := make([]string, len(values))
	for i, val := range values {
		parts := make([]string, 0, len(entities[i]))
		for _, tv := range entities[i] {
			parts = append(parts, Stringify(tv))
		}
		items[i] = fmt.Sprintf("%v=%s", val, strings.Join(parts, "|"))
	}
	slices.Sort(items)
	return fmt.Sprintf("%s %v %v", name, tags, items), nil
}

// MergeTopNResults lets the export service merge Top-N result versions without importing this package.
func (s *standalone) MergeTopNResults(entity []*modelv1.TagValue, timestamp, leftVersion, rightVersion int64, left, right []byte) ([]byte, error) {
	return MergeTopNResults(entity, timestamp, leftVersion, rightVersion, left, right)
}

// MergeTopNResults is the data node counterpart of (*standalone).MergeTopNResults.
func (s *dataSVC) MergeTopNResults(entity []*modelv1.TagValue, timestamp, leftVersion, rightVersion int64, left, right []byte) ([]byte, error) {
	return MergeTopNResults(entity, timestamp, leftVersion, rightVersion, left, right)
}
