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

// Package v1 implements helpers to access data defined by API v1.
package v1

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// FindTagByName finds TagSpec in several tag families by its name.
// The tag name should be unique in these families.
func FindTagByName(families []*databasev1.TagFamilySpec, tagName string) (int, int, *databasev1.TagSpec) {
	for fi, family := range families {
		for ti, tag := range family.Tags {
			if tagName == tag.GetName() {
				return fi, ti, tag
			}
		}
	}
	return 0, 0, nil
}

func tagValueTypeConv(tagValue *modelv1.TagValue) (tagType databasev1.TagType, isNull bool) {
	switch tagValue.GetValue().(type) {
	case *modelv1.TagValue_Int:
		return databasev1.TagType_TAG_TYPE_INT, false
	case *modelv1.TagValue_Str:
		return databasev1.TagType_TAG_TYPE_STRING, false
	case *modelv1.TagValue_IntArray:
		return databasev1.TagType_TAG_TYPE_INT_ARRAY, false
	case *modelv1.TagValue_StrArray:
		return databasev1.TagType_TAG_TYPE_STRING_ARRAY, false
	case *modelv1.TagValue_BinaryData:
		return databasev1.TagType_TAG_TYPE_DATA_BINARY, false
	case *modelv1.TagValue_Null:
		return databasev1.TagType_TAG_TYPE_UNSPECIFIED, true
	}
	return databasev1.TagType_TAG_TYPE_UNSPECIFIED, false
}

// FieldValueTypeConv recognizes the field type from its value.
func FieldValueTypeConv(fieldValue *modelv1.FieldValue) (tagType databasev1.FieldType, isNull bool) {
	switch fieldValue.GetValue().(type) {
	case *modelv1.FieldValue_Int:
		return databasev1.FieldType_FIELD_TYPE_INT, false
	case *modelv1.FieldValue_Float:
		return databasev1.FieldType_FIELD_TYPE_FLOAT, false
	case *modelv1.FieldValue_Str:
		return databasev1.FieldType_FIELD_TYPE_STRING, false
	case *modelv1.FieldValue_BinaryData:
		return databasev1.FieldType_FIELD_TYPE_DATA_BINARY, false
	case *modelv1.FieldValue_Null:
		return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, true
	}
	return databasev1.FieldType_FIELD_TYPE_UNSPECIFIED, false
}
