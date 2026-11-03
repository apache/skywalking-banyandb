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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// t15NaNBits records independently authored wire payloads without asking JSON to encode a nonfinite number.
type t15NaNBits uint64

// T15FixtureName isolates independently written tuple and signed-zero witnesses.
const T15FixtureName = "ql_t15"

func t15Rows() []fixtureRow {
	rows := []fixtureRow{
		{RID: 1, Host: "ab", Zone: "c", F: float64(0), V: int64(0)},
		{RID: 2, Host: "a", Zone: "bc", F: math.Copysign(0, -1), V: int64(0)},
		{RID: 3, Host: "ab", Zone: "c"},
		{RID: 4, Host: "a", Zone: "bc"},
		{RID: 5, Host: "null"},
		{RID: 6, Host: "empty", Zone: ""},
		{RID: 7, Host: "x", Zone: "x", F: t15NaNBits(0x7ff8000000000001)},
		{RID: 8, Host: "y", Zone: "y", F: t15NaNBits(0x7ff80000000000ff)},
	}
	// Each tuple repeats at another timestamp and series; cardinality exceeds
	// the native default limit and input spans multiple columnar batches.
	for group := range 129 {
		for range 17 {
			rows = append(rows, fixtureRow{RID: int64(len(rows) + 1), Host: fmt.Sprintf("tuple-%03d", group), Zone: "x", V: int64(group), F: float64(group)})
		}
	}
	for ordinal := range rows {
		rows[ordinal].Millis = int64(ordinal * 100)
	}
	return rows
}

// T15FixtureSchema uses unique series so projected duplicates cross source identities.
func T15FixtureSchema() *databasev1.Measure {
	schema := FixtureSchema()
	schema.Metadata.Name = T15FixtureName
	return schema
}

// T15FixtureWrites specifies input independently of the DISTINCT response oracle.
func T15FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	writes := FixtureWrites(base)
	template := writes[0]
	var result []*measurev1.WriteRequest
	for ordinal, row := range t15Rows() {
		request := &measurev1.WriteRequest{Metadata: T15FixtureSchema().Metadata, MessageId: uint64(ordinal + 1), DataPointSpec: template.DataPointSpec}
		point := &measurev1.DataPointValue{}
		point.Timestamp = timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond))
		point.TagFamilies = []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
			tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone), tagValue(row.Labels), tagValue(row.Nums),
		}}}
		floating := row.F
		if bits, ok := floating.(t15NaNBits); ok {
			floating = math.Float64frombits(uint64(bits))
		}
		point.Fields = []*modelv1.FieldValue{fieldValue(row.V), fieldValue(floating)}
		// Reuse the existing writer conversion, not generated query results.
		request.DataPoint = point
		result = append(result, request)
	}
	return result
}

func t15OracleRows() []fixtureRow {
	rows := t15Rows()
	for ordinal := range rows {
		if bits, ok := rows[ordinal].F.(t15NaNBits); ok {
			rows[ordinal].F = math.Float64frombits(uint64(bits))
		}
	}
	return rows
}
