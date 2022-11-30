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

package v1

import (
	database_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	model_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func FindTagByName(families []*database_v1.TagFamilySpec, tagName string) (int, int, *database_v1.TagSpec) {
	for fi, family := range families {
		for ti, tag := range family.Tags {
			if tagName == tag.GetName() {
				return fi, ti, tag
			}
		}
	}
	return 0, 0, nil
}

func TagValueTypeConv(tagValue *model_v1.TagValue) (tagType database_v1.TagType, isNull bool) {
	switch tagValue.GetValue().(type) {
	case *model_v1.TagValue_Int:
		return database_v1.TagType_TAG_TYPE_INT, false
	case *model_v1.TagValue_Str:
		return database_v1.TagType_TAG_TYPE_STRING, false
	case *model_v1.TagValue_IntArray:
		return database_v1.TagType_TAG_TYPE_INT_ARRAY, false
	case *model_v1.TagValue_StrArray:
		return database_v1.TagType_TAG_TYPE_STRING_ARRAY, false
	case *model_v1.TagValue_BinaryData:
		return database_v1.TagType_TAG_TYPE_DATA_BINARY, false
	case *model_v1.TagValue_Id:
		return database_v1.TagType_TAG_TYPE_ID, false
	case *model_v1.TagValue_Null:
		return database_v1.TagType_TAG_TYPE_UNSPECIFIED, true
	}
	return database_v1.TagType_TAG_TYPE_UNSPECIFIED, false
}

func FieldValueTypeConv(tagValue *model_v1.FieldValue) (tagType database_v1.FieldType, isNull bool) {
	switch tagValue.GetValue().(type) {
	case *model_v1.FieldValue_Int:
		return database_v1.FieldType_FIELD_TYPE_INT, false
	case *model_v1.FieldValue_Str:
		return database_v1.FieldType_FIELD_TYPE_STRING, false
	case *model_v1.FieldValue_BinaryData:
		return database_v1.FieldType_FIELD_TYPE_DATA_BINARY, false
	case *model_v1.FieldValue_Null:
		return database_v1.FieldType_FIELD_TYPE_UNSPECIFIED, true
	}
	return database_v1.FieldType_FIELD_TYPE_UNSPECIFIED, false
}

func ParseMaxModRevision(indexRules []*database_v1.IndexRule) (maxRevisionForIdxRules int64) {
	maxRevisionForIdxRules = int64(0)
	for _, idxRule := range indexRules {
		if idxRule.GetMetadata().GetModRevision() > maxRevisionForIdxRules {
			maxRevisionForIdxRules = idxRule.GetMetadata().GetModRevision()
		}
	}
	return
}
