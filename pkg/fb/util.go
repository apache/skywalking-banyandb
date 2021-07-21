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

package fb

import (
	flatbuffers "github.com/google/flatbuffers/go"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func Transform(entityValue *v1.EntityValue, fieldsIndices []FieldEntry, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	fieldsLen := len(fieldsIndices)
	result := make([]flatbuffers.UOffsetT, 0, fieldsLen)

	VisitFields(entityValue, fieldsIndices, func(key string, valueType v1.ValueType, value interface{}) {
		keyOffset := builder.CreateString(key)
		var pairOffset flatbuffers.UOffsetT
		var typedPair v1.TypedPair
		switch valueType {
		case v1.ValueTypeNONE:
			typedPair = v1.TypedPairNONE
			pairOffset = 0
		case v1.ValueTypeStr, v1.ValueTypeStrArray:
			typedPair = v1.TypedPairStrPair
			stringValue, isStr := value.(*v1.Str)
			var l int
			var stringArrayValue *v1.StrArray
			if isStr {
				l = 1
			} else {
				stringArrayValue = value.(*v1.StrArray)
				l = stringArrayValue.ValueLength()
			}
			strPos := make([]flatbuffers.UOffsetT, 0, l)
			for j := 0; j < l; j++ {
				var o flatbuffers.UOffsetT
				if isStr {
					o = builder.CreateByteString(stringValue.Value())
				} else {
					o = builder.CreateByteString(stringArrayValue.Value(j))
				}
				strPos = append(strPos, o)
			}
			v1.StrArrayStartValueVector(builder, l)
			for j := l - 1; j >= 0; j-- {
				builder.PrependUOffsetT(strPos[j])
			}
			strArray := builder.EndVector(l)
			v1.StrPairStart(builder)
			v1.StrPairAddKey(builder, keyOffset)
			v1.StrPairAddValues(builder, strArray)
			pairOffset = v1.StrPairEnd(builder)
		case v1.ValueTypeInt, v1.ValueTypeIntArray:
			typedPair = v1.TypedPairIntPair
			intValue, isInt := value.(*v1.Int)
			var l int
			var intArrayValue *v1.IntArray
			if isInt {
				l = 1
			} else {
				intArrayValue = value.(*v1.IntArray)
				l = intArrayValue.ValueLength()
			}
			v1.IntArrayStartValueVector(builder, l)
			for j := l - 1; j >= 0; j-- {
				var v int64
				if isInt {
					v = intValue.Value()
				} else {
					v = intArrayValue.Value(j)
				}
				builder.PrependInt64(v)
			}
			intArray := builder.EndVector(l)
			v1.IntPairStart(builder)
			v1.IntPairAddKey(builder, keyOffset)
			v1.IntPairAddValues(builder, intArray)
			pairOffset = v1.IntPairEnd(builder)
		}
		if pairOffset <= 0 {
			return
		}
		v1.PairStart(builder)
		v1.PairAddPairType(builder, typedPair)
		v1.PairAddPair(builder, pairOffset)
		result = append(result, v1.PairEnd(builder))
	})
	l := len(result)
	if l < 1 {
		return 0
	}
	v1.EntityStartFieldsVector(builder, l)
	for i := l - 1; i >= 0; i-- {
		builder.PrependUOffsetT(result[i])
	}
	return builder.EndVector(l)
}

func CopyFields(entityValue *v1.EntityValue, builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	fieldsLen := entityValue.FieldsLength()
	result := make([]flatbuffers.UOffsetT, 0, fieldsLen)
	VisitFields(entityValue, nil, func(_ string, valueType v1.ValueType, value interface{}) {
		var valueOffset flatbuffers.UOffsetT
		switch valueType {
		case v1.ValueTypeNONE:
			valueOffset = 0
		case v1.ValueTypeStr:
			stringValue := value.(*v1.Str)
			pos := builder.CreateByteString(stringValue.Value())
			v1.StrStart(builder)
			v1.StrAddValue(builder, pos)
			valueOffset = v1.StrEnd(builder)
		case v1.ValueTypeInt:
			intValue := value.(*v1.Int)
			v1.IntStart(builder)
			v1.IntAddValue(builder, intValue.Value())
			valueOffset = v1.IntEnd(builder)
		case v1.ValueTypeStrArray:
			stringArrayValue := value.(*v1.StrArray)
			l := stringArrayValue.ValueLength()
			v1.StrArrayStartValueVector(builder, l)
			strPos := make([]flatbuffers.UOffsetT, 0, l)
			for j := 0; j < l; j++ {
				strPos = append(strPos, builder.CreateByteString(stringArrayValue.Value(j)))
			}
			for j := l; j >= 0; j-- {
				builder.PrependUOffsetT(strPos[j])
			}
			valueOffset = builder.EndVector(l)
		case v1.ValueTypeIntArray:
			intArrayValue := value.(*v1.IntArray)
			l := intArrayValue.ValueLength()
			v1.IntArrayStartValueVector(builder, l)
			for j := l; j >= 0; j-- {
				builder.PrependInt64(intArrayValue.Value(j))
			}
			valueOffset = builder.EndVector(l)
		}
		v1.FieldStart(builder)
		v1.FieldAddValueType(builder, valueType)
		if valueOffset > 0 {
			v1.FieldAddValue(builder, valueOffset)
		}
		result = append(result, v1.FieldEnd(builder))
	})

	v1.EntityValueStartFieldsVector(builder, fieldsLen)
	for i := fieldsLen - 1; i >= 0; i-- {
		builder.PrependUOffsetT(result[i])
	}
	o := builder.EndVector(fieldsLen)
	entityID := builder.CreateByteString(entityValue.EntityId())
	v1.EntityValueStart(builder)
	v1.EntityValueAddFields(builder, o)
	v1.EntityValueAddEntityId(builder, entityID)
	v1.EntityValueAddTimestampNanoseconds(builder, entityValue.TimestampNanoseconds())
	return v1.EntityValueEnd(builder)
}

func VisitFields(entityValue *v1.EntityValue, fieldsIndices []FieldEntry, visit func(key string, valueType v1.ValueType, value interface{})) {
	var fieldsLen int
	if fieldsIndices == nil {
		fieldsLen = entityValue.FieldsLength()
	} else {
		fieldsLen = len(fieldsIndices)
	}
	for i := 0; i < fieldsLen; i++ {
		var index int
		var key string
		if fieldsIndices == nil {
			index = i
		} else {
			index = fieldsIndices[i].Index
			key = fieldsIndices[i].Key
		}
		if index >= entityValue.FieldsLength() {
			visit(key, v1.ValueTypeNONE, nil)
			continue
		}
		f := &v1.Field{}
		if !entityValue.Fields(f, index) {
			continue
		}

		unionTable := new(flatbuffers.Table)
		f.Value(unionTable)
		switch f.ValueType() {
		case v1.ValueTypeNONE:
			visit(key, f.ValueType(), nil)
		case v1.ValueTypeStr:
			stringValue := new(v1.Str)
			stringValue.Init(unionTable.Bytes, unionTable.Pos)
			visit(key, f.ValueType(), stringValue)
		case v1.ValueTypeInt:
			intValue := new(v1.Int)
			intValue.Init(unionTable.Bytes, unionTable.Pos)
			visit(key, f.ValueType(), intValue)
		case v1.ValueTypeStrArray:
			stringArrayValue := new(v1.StrArray)
			stringArrayValue.Init(unionTable.Bytes, unionTable.Pos)
			visit(key, f.ValueType(), stringArrayValue)
		case v1.ValueTypeIntArray:
			intArrayValue := new(v1.IntArray)
			intArrayValue.Init(unionTable.Bytes, unionTable.Pos)
			visit(key, f.ValueType(), intArrayValue)
		}
	}
}

type FieldEntry struct {
	Key   string
	Index int
}
