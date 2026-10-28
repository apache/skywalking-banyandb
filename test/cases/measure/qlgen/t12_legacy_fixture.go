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

// T12LegacyFixtureName isolates the legacy heterogeneous-schema regression.
const T12LegacyFixtureName = "ql_t12_legacy"

// T12LegacyFixtureGroups returns independent source groups with compatible selected columns.
func T12LegacyFixtureGroups() []string { return []string{"ql_t12_legacy_a", "ql_t12_legacy_b"} }

type t12LegacyRow struct {
	Unused    any
	UnusedTag any
	Group     string
	Host      string
	Kept      int64
	Millis    int64
}

func t12LegacyRows() []t12LegacyRow {
	groups := T12LegacyFixtureGroups()
	return []t12LegacyRow{
		{Group: groups[0], Host: "legacy-a", Kept: 11, Millis: 0, Unused: int64(101), UnusedTag: "first"},
		{Group: groups[1], Host: "legacy-b", Kept: 21, Millis: 1000, Unused: 1.25, UnusedTag: int64(201)},
	}
}

// T12LegacyFixtureSchema varies only columns that legacy requests do not reference.
func T12LegacyFixtureSchema(group string) *databasev1.Measure {
	unusedField := databasev1.FieldType_FIELD_TYPE_INT
	unusedTag := databasev1.TagType_TAG_TYPE_STRING
	if group == T12LegacyFixtureGroups()[1] {
		unusedField = databasev1.FieldType_FIELD_TYPE_FLOAT
		unusedTag = databasev1.TagType_TAG_TYPE_INT
	}
	field := func(name string, typ databasev1.FieldType) *databasev1.FieldSpec {
		return &databasev1.FieldSpec{
			Name: name, FieldType: typ,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}
	}
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: T12LegacyFixtureName, Group: group}, Interval: "1s",
		Entity: &databasev1.Entity{TagNames: []string{"host"}},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
			{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING}, {Name: "unused_tag", Type: unusedTag},
		}}},
		Fields: []*databasev1.FieldSpec{field("kept", databasev1.FieldType_FIELD_TYPE_INT), field("unused", unusedField)},
	}
}

// T12LegacyFixtureWrites materializes typed source literals for one group.
func T12LegacyFixtureWrites(group string, base time.Time) []*measurev1.WriteRequest {
	var writes []*measurev1.WriteRequest
	for ordinal, row := range t12LegacyRows() {
		if row.Group != group {
			continue
		}
		writes = append(writes, &measurev1.WriteRequest{
			Metadata: T12LegacyFixtureSchema(group).GetMetadata(), MessageId: uint64(ordinal + 1),
			DataPointSpec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{Name: "default", TagNames: []string{"host", "unused_tag"}}}, FieldNames: []string{"kept", "unused"},
			},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(base.Add(time.Duration(row.Millis) * time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{tagValue(row.Host), tagValue(row.UnusedTag)}}},
				Fields:      []*modelv1.FieldValue{fieldValue(row.Kept), fieldValue(row.Unused)},
			},
		})
	}
	return writes
}
