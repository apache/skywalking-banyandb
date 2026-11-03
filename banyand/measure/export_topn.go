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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// MergeTopNResults combines two stored field values of one data point of the Top-N result
// measure the way a query and a part merge do. entity holds the data point's entity values
// in the order of TopNTagNames.
func MergeTopNResults(entity []*modelv1.TagValue, timestamp, leftVersion, rightVersion int64, left, right []byte) ([]byte, error) {
	if len(entity) != len(TopNTagNames) {
		return nil, fmt.Errorf("a %s data point has %d entity values, want %d", TopNSchemaName, len(entity), len(TopNTagNames))
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

// MergeTopNResults lets the export service merge Top-N result versions without importing this package.
func (s *standalone) MergeTopNResults(entity []*modelv1.TagValue, timestamp, leftVersion, rightVersion int64, left, right []byte) ([]byte, error) {
	return MergeTopNResults(entity, timestamp, leftVersion, rightVersion, left, right)
}

// MergeTopNResults is the data node counterpart of (*standalone).MergeTopNResults.
func (s *dataSVC) MergeTopNResults(entity []*modelv1.TagValue, timestamp, leftVersion, rightVersion int64, left, right []byte) ([]byte, error) {
	return MergeTopNResults(entity, timestamp, leftVersion, rightVersion, left, right)
}
