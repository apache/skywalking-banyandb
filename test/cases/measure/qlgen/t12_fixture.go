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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T12FixtureName identifies nullable F examples and a tail beyond the native default limit.
const T12FixtureName = "ql_t12"

func t12Rows() []fixtureRow {
	rows := append([]fixtureRow(nil), fixtureRows()[:6]...)
	for ordinal := range 256 {
		rows = append(rows, fixtureRow{
			Millis: 10000 + int64(ordinal)*10, RID: 1000 + int64(ordinal), Host: fmt.Sprintf("late-%d", ordinal%4),
			Zone: "tail", Labels: []string{"late"}, Nums: []int64{int64(ordinal)}, V: int64(ordinal), F: float64(ordinal) / 4,
		})
	}
	return rows
}

// T12FixtureSchema returns the independent field predicate fixture schema.
func T12FixtureSchema() *databasev1.Measure {
	schema := FixtureSchema()
	schema.Metadata = &commonv1.Metadata{Name: T12FixtureName, Group: FixtureGroup}
	return schema
}

// T12FixtureWrites materializes source rows only; expected query values use the scalar oracle.
func T12FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	rows := t12Rows()
	writes := make([]*measurev1.WriteRequest, 0, len(rows))
	for ordinal, row := range rows {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: T12FixtureSchema().GetMetadata(), MessageId: uint64(ordinal + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"rid", "host", "zone", "labels", "nums"}}},
				FieldNames:    []string{"v", "f"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone), tagValue(row.Labels), tagValue(row.Nums),
				}}},
				Fields: []*modelv1.FieldValue{fieldValue(row.V), fieldValue(row.F)},
			},
		})
	}
	return writes
}

func t12ReadinessCase() (Case, error) {
	var points []any
	for _, row := range t12Rows() {
		points = append(points, projectRow(row, []string{"rid", "host", "zone", "labels", "nums", "v", "f"}, true))
	}
	query := "SELECT rid, host, zone, labels, nums, v::FIELD, f::FIELD FROM " + FixtureGroup + "." + T12FixtureName +
		" WHERE time >= '" + timePlaceholder(-1) + "' AND time < '" + timePlaceholder(13000) + "' ORDER BY TIME ASC"
	c, err := t07ExpectedCase("t12_ready", query, points, []string{"t12.fixture"})
	c.Ticket, c.Ordered = "T12", true
	return c, err
}
