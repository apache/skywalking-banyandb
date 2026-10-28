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

// Package topnschema defines the _top_n_result measure every measure group gets. It only
// depends on the API types, so clients can predict the measure the server creates.
package topnschema

import (
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

const (
	// Name is the name of the top n result measure.
	Name = "_top_n_result"
	// TagFamily is the tag family name of the top n result measure.
	TagFamily = "_topN"
	// FieldName is the field name of the top n result measure.
	FieldName = "value"
)

// TagNames are the tag names of the top n result measure, its entity in this order.
var TagNames = []string{"name", "direction", "group", "parameters"}

// Metadata returns the metadata of the top n result measure of a group.
func Metadata(group string) *commonv1.Metadata {
	return &commonv1.Metadata{Name: Name, Group: group}
}

// Measure returns the top n result measure the server creates with md.
func Measure(md *commonv1.Metadata) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: md,
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: TagFamily,
				Tags: []*databasev1.TagSpec{
					{Name: TagNames[0], Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: TagNames[1], Type: databasev1.TagType_TAG_TYPE_INT},
					{Name: TagNames[2], Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: TagNames[3], Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{{
			Name:              FieldName,
			FieldType:         databasev1.FieldType_FIELD_TYPE_DATA_BINARY,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
		Entity: &databasev1.Entity{TagNames: TagNames},
	}
}
