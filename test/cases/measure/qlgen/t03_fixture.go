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

package qlgen

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// T03FixtureName identifies the independent extended raw-query fixture.
const T03FixtureName = "ql_t03"

func t03Rows() []fixtureRow {
	rows := fixtureRows()
	for index := 8; index < 129; index++ {
		rows = append(rows, fixtureRow{Millis: int64(index * 1000), RID: int64(index + 1), Host: "extended", Zone: "tail", V: int64(index), F: float64(index) + 0.5})
	}
	return rows
}

// T03FixtureSchema returns a multi-family schema with a deliberate tag/field ambiguity.
func T03FixtureSchema() *databasev1.Measure {
	schema := FixtureSchema()
	schema.Metadata = &commonv1.Metadata{Name: T03FixtureName, Group: FixtureGroup}
	schema.Entity = &databasev1.Entity{TagNames: []string{"host"}}
	schema.TagFamilies = []*databasev1.TagFamilySpec{
		{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "rid", Type: databasev1.TagType_TAG_TYPE_INT},
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "zone", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "clash", Type: databasev1.TagType_TAG_TYPE_STRING},
		}},
		{Name: "extra", Tags: []*databasev1.TagSpec{{Name: "location", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		{Name: "__ql", Tags: []*databasev1.TagSpec{{Name: "clock", Type: databasev1.TagType_TAG_TYPE_STRING}}},
	}
	schema.Fields = append(schema.Fields, &databasev1.FieldSpec{
		Name: "clash", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
		EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	})
	return schema
}

// T03FixtureWrites constructs the 129-row fixture without issuing queries.
func T03FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	rows := t03Rows()
	writes := make([]*measurev1.WriteRequest, 0, len(rows))
	for index, row := range rows {
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: T03FixtureSchema().GetMetadata(), MessageId: uint64(index + 1),
			DataPointSpec: &measurev1.DataPointSpec{TagFamilySpec: []*measurev1.TagFamilySpec{
				{Name: "default", TagNames: []string{"rid", "host", "zone", "clash"}},
				{Name: "extra", TagNames: []string{"location"}},
				{Name: "__ql", TagNames: []string{"clock"}},
			}, FieldNames: []string{"v", "f", "clash"}},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{
					{Tags: []*modelv1.TagValue{tagValue(row.RID), tagValue(row.Host), tagValue(row.Zone), tagValue("tag-clash")}},
					{Tags: []*modelv1.TagValue{tagValue("location-" + row.Host)}},
					{Tags: []*modelv1.TagValue{tagValue("stored-clock")}},
				}, Fields: []*modelv1.FieldValue{fieldValue(row.V), fieldValue(row.F), fieldValue(row.RID * 10)},
			},
		})
	}
	return writes
}
