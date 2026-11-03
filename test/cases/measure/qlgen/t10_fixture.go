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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T10FixtureName identifies the independent expression and lexer fixture.
const T10FixtureName = "ql_t10"

func t10Rows() []fixtureRow { return fixtureRows()[:6] }

// T10FixtureSchema keeps arrays unindexed and includes accepted legacy field names.
func T10FixtureSchema() *databasev1.Measure {
	schema := FixtureSchema()
	schema.Metadata = &commonv1.Metadata{Name: T10FixtureName, Group: FixtureGroup}
	for _, name := range []string{"metric-value", "rate_total"} {
		schema.Fields = append(schema.Fields, &databasev1.FieldSpec{
			Name: name, FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		})
	}
	return schema
}

// T10FixtureWrites writes the exact six F rows without querying an execution engine.
func T10FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	writes := make([]*measurev1.WriteRequest, 0, len(t10Rows()))
	for ordinal, row := range t10Rows() {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: T10FixtureSchema().GetMetadata(), MessageId: uint64(ordinal + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"rid", "host", "zone", "labels", "nums"}}},
				FieldNames:    []string{"v", "f", "metric-value", "rate_total"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone), tagValue(row.Labels), tagValue(row.Nums),
				}}},
				Fields: []*modelv1.FieldValue{fieldValue(row.V), fieldValue(row.F), fieldValue(row.V), fieldValue(row.V)},
			},
		})
	}
	return writes
}

func t10ReadinessCase() (Case, error) {
	var points []any
	for _, row := range t10Rows() {
		point := projectRow(row, []string{"rid", "host", "zone", "labels", "nums", "v", "f"}, true)
		fields := point["fields"].([]any)
		for _, name := range []string{"metric-value", "rate_total"} {
			fields = append(fields, map[string]any{"name": name, "value": jsonValue(row.V)})
		}
		point["fields"] = fields
		points = append(points, point)
	}
	c, err := t07ExpectedCase("t10_ready", "SELECT rid, host, zone, labels, nums, v::FIELD, f::FIELD, metric-value::FIELD, rate_total::FIELD FROM "+
		FixtureGroup+"."+T10FixtureName+" WHERE time >= '"+timePlaceholder(-1)+"' AND time < '"+
		timePlaceholder(6000)+"' ORDER BY TIME ASC", points, []string{"t10.fixture"})
	c.Ticket, c.Ordered = "T10", true
	return c, err
}
