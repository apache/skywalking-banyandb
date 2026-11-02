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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// P1Group names the isolated directory-list capacity fixture.
const P1Group = "qlperf"

// P1FixtureName names the neutral activity measure.
const P1FixtureName = "activity"

// P1BaseTime supplies a fixed identity for fixture hashing, independent of execution time.
func P1BaseTime() time.Time { return time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC) }

// P1FixtureGroup creates two naturally routed shards without replicas.
func P1FixtureGroup() *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: P1Group}, Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 2, Replicas: 0, SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

// P1FixtureSchema leaves the routing entity unindexed and indexes only service IDs.
func P1FixtureSchema() *databasev1.Measure {
	s := ParentChildFixtureSchema(P1FixtureName)
	s.Metadata.Group = P1Group
	s.TagFamilies = []*databasev1.TagFamilySpec{
		{Name: "storage-only", Tags: []*databasev1.TagSpec{{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
		{Name: "searchable", Tags: []*databasev1.TagSpec{{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING}}},
	}
	for _, name := range []string{"summation", "count"} {
		s.Fields = append(s.Fields, &databasev1.FieldSpec{
			Name: name, FieldType: databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		})
	}
	return s
}

// P1IndexRules returns the single whole-value service index.
func P1IndexRules() []*databasev1.IndexRule {
	return []*databasev1.IndexRule{{
		Metadata: &commonv1.Metadata{Name: "activity_service", Group: P1Group},
		Tags:     []string{"service_id"}, Type: databasev1.IndexRule_TYPE_INVERTED, Analyzer: "keyword", NoSort: true,
	}}
}

// P1IndexBinding binds only the service index before writes.
func P1IndexBinding() *databasev1.IndexRuleBinding {
	b := ParentChildIndexBinding(P1FixtureName)
	b.Metadata.Group = P1Group
	b.Rules = []string{"activity_service"}
	return b
}

// P1FixtureWrites deterministically generates 1024 services, five entities and two timestamps.
func P1FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	writes := make([]*measurev1.WriteRequest, 0, 10240)
	for service := 0; service < 1024; service++ {
		for entity := 0; entity < 5; entity++ {
			value := int64(1)
			if entity == 4 {
				value = 0
			}
			for bucket := 0; bucket < 2; bucket++ {
				writes = append(writes, &measurev1.WriteRequest{
					Metadata: P1FixtureSchema().Metadata, MessageId: uint64(len(writes) + 1),
					DataPointSpec: &measurev1.DataPointSpec{
						TagFamilySpec: []*measurev1.TagFamilySpec{
							{Name: "storage-only", TagNames: []string{"entity_id"}}, {Name: "searchable", TagNames: []string{"service_id"}},
						},
						FieldNames: []string{"value", "summation", "count"},
					},
					DataPoint: &measurev1.DataPointValue{
						Timestamp: timestamppb.New(base.Add(time.Duration(bucket) * time.Second)),
						TagFamilies: []*modelv1.TagFamilyForWrite{
							{Tags: []*modelv1.TagValue{tagValue(fmt.Sprintf("entity-%04d-%d", service, entity))}},
							{Tags: []*modelv1.TagValue{tagValue(fmt.Sprintf("present-%04d", service))}},
						},
						Fields: []*modelv1.FieldValue{fieldValue(value), fieldValue(value), fieldValue(int64(1))},
					},
				})
			}
		}
	}
	return writes
}

// P1FixtureDigest freezes schema, index configuration and deterministic row values.
func P1FixtureDigest() string {
	data, err := json.Marshal([]any{P1FixtureGroup(), P1FixtureSchema(), P1IndexRules(), P1IndexBinding(), P1FixtureWrites(P1BaseTime())})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// P1List holds present services fixed while growing an absent-ID tail.
func P1List(size, present int) []string {
	result := make([]string, size)
	for i := range result {
		if i < present {
			result[i] = fmt.Sprintf("present-%04d", i)
		} else {
			result[i] = fmt.Sprintf("absent-%05d", i)
		}
	}
	return result
}

// P1Memberships preserves 10000 total bindings and two matching CASE branches.
func P1Memberships(branches int, variant string) [][]string {
	result := make([][]string, branches)
	for service := 0; service < 1024; service++ {
		branch := 1
		if service < 256 {
			branch = 0
		}
		if variant == "swap" {
			branch = 1 - branch
		}
		result[branch] = append(result[branch], fmt.Sprintf("present-%04d", service))
	}
	total := 1024
	if variant == "overlap" {
		result[0] = append(result[0], "present-0256")
		total++
	}
	for i := total; i < 10000; i++ {
		branch := (i - total) % branches
		result[branch] = append(result[branch], fmt.Sprintf("absent-%05d", i))
	}
	return result
}
