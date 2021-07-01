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

package grpc

import (
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func SerializeQuery(criteria *v1.EntityCriteria) (*flatbuffers.Builder, error) {
	builder := flatbuffers.NewBuilder(0)
	// Serialize timeRange
	timeRange := criteria.TimestampNanoseconds(nil)
	v1.RangeQueryStart(builder)
	v1.RangeQueryAddBegin(builder, timeRange.Begin())
	v1.RangeQueryAddEnd(builder, timeRange.End())
	v1.RangeQueryEnd(builder)
	// Serialize projection
	var projStr []flatbuffers.UOffsetT
	proj := criteria.Projection(nil)
	if proj != nil {
		for i := 0; i < proj.KeyNamesLength(); i++ {
			keyName := builder.CreateString(string(proj.KeyNames(i)))
			projStr = append(projStr, keyName)
		}
	}
	v1.ProjectionStart(builder)
	for p := range projStr {
		v1.ProjectionAddKeyNames(builder, flatbuffers.UOffsetT(p))
	}
	v1.ProjectionEnd(builder)
	// Serialize MetaData
	metaData := criteria.Metadata(nil)
	group, name := builder.CreateString(string(metaData.Group())), builder.CreateString(string(metaData.Name()))
	v1.MetadataStart(builder)
	v1.MetadataAddGroup(builder, group)
	v1.MetadataAddName(builder, name)
	v1.MetadataEnd(builder)
	// Serialize OrderBy
	queryOrder := criteria.OrderBy(nil)
	if queryOrder != nil {
		keyName := builder.CreateString(string(queryOrder.KeyName()))
		v1.QueryOrderStart(builder)
		v1.QueryOrderAddKeyName(builder, keyName)
		v1.QueryOrderAddSort(builder, queryOrder.Sort())
		v1.QueryOrderEnd(builder)
	}
	// Serialize Fields
	var PairList []flatbuffers.UOffsetT
	if criteria.FieldsLength() > 0 {
		for i := 0; i < criteria.FieldsLength(); i++ {
			var pairQuery v1.PairQuery
			if ok := criteria.Fields(&pairQuery, i); ok {
				op := pairQuery.Op()
				pair := pairQuery.Condition(nil)
				unionPairTable := new(flatbuffers.Table)
				if ok := pair.Pair(unionPairTable); ok {
					if pair.PairType() == v1.TypedPairStrPair {
						unionStrPair := new(v1.StrPair)
						unionStrPair.Init(unionPairTable.Bytes, unionPairTable.Pos)
						l := unionStrPair.ValuesLength()
						// Serialize Pair
						v1.PairStart(builder)
						v1.PairAddPairType(builder, v1.TypedPairStrPair)
						v1.PairEnd(builder)
						var offsets []flatbuffers.UOffsetT
						for j := 0; j < l; j++ {
							v := builder.CreateString(string(unionStrPair.Values(j)))
							v1.StrPairStart(builder)
							v1.StrPairAddValues(builder, v)
							offset := v1.StrPairEnd(builder)
							offsets = append(offsets, offset)
						}
						v1.StrPairStartValuesVector(builder, l)
						for o := range offsets {
							builder.PrependUOffsetT(flatbuffers.UOffsetT(o))
						}
						f := builder.EndVector(l)
						PairList = append(PairList, f)
					} else if pair.PairType() == v1.TypedPairIntPair {
						unionIntPair := new(v1.IntPair)
						unionIntPair.Init(unionPairTable.Bytes, unionPairTable.Pos)
						// Serialize Pair
						v1.PairStart(builder)
						v1.PairAddPairType(builder, v1.TypedPairStrPair)
						v1.PairEnd(builder)
						l := unionIntPair.ValuesLength()
						var offsets []flatbuffers.UOffsetT
						for j := 0; j < l; j++ {
							v1.IntPairStart(builder)
							v1.IntPairAddValues(builder, flatbuffers.UOffsetT(unionIntPair.Values(j)))
							offset := v1.IntPairEnd(builder)
							offsets = append(offsets, offset)
						}
						v1.IntPairStartValuesVector(builder, l)
						for o := range offsets {
							builder.PrependUOffsetT(flatbuffers.UOffsetT(o))
						}
						f := builder.EndVector(l)
						PairList = append(PairList, f)
					}
					// Serialize PairQuery
					v1.PairQueryStart(builder)
					v1.PairQueryAddOp(builder, op)
					for p := range PairList {
						v1.PairQueryAddCondition(builder, flatbuffers.UOffsetT(p))
					}
					v1.PairQueryEnd(builder)
				}
			}
		}
	}
	// Serialize EntityCriteria
	v1.EntityCriteriaStartFieldsVector(builder, len(PairList))
	for val := range PairList {
		builder.PrependUOffsetT(flatbuffers.UOffsetT(val))
	}
	PairListP := builder.EndVector(len(PairList))
	offset := criteria.Offset()
	limit := criteria.Limit()
	v1.EntityCriteriaStart(builder)
	v1.EntityCriteriaAddOffset(builder, offset)
	v1.EntityCriteriaAddLimit(builder, limit)
	v1.EntityCriteriaAddFields(builder, PairListP)
	position := v1.EntityCriteriaEnd(builder)
	builder.Finish(position)

	return builder, nil
}

func SerializeWrite(writeEntity *v1.WriteEntity) (*flatbuffers.Builder, error) {
	builder := flatbuffers.NewBuilder(0)
	metaData := writeEntity.MetaData(nil)
	entityValue := writeEntity.Entity(nil)
	// Serialize MetaData
	group, name := builder.CreateString(string(metaData.Group())), builder.CreateString(string(metaData.Name()))
	v1.MetadataStart(builder)
	v1.MetadataAddGroup(builder, group)
	v1.MetadataAddName(builder, name)
	v1.MetadataEnd(builder)
	// Serialize Fields
	var fieldList []flatbuffers.UOffsetT
	for i := 0; i < 1; i++ {
		var field v1.Field
		var str string
		if ok := entityValue.Fields(&field, i); ok {
			unionValueType := new(flatbuffers.Table)
			if field.Value(unionValueType) {
				valueType := field.ValueType()
				if valueType == v1.ValueTypeString {
					unionStr := new(v1.String)
					unionStr.Init(unionValueType.Bytes, unionValueType.Pos)
					v1.FieldStart(builder)
					v1.FieldAddValueType(builder, v1.ValueTypeString)
					v1.FieldEnd(builder)
					str = string(unionStr.Value())
					f := builder.CreateString(str)
					fieldList = append(fieldList, f)
				} else if valueType == v1.ValueTypeInt {
					unionInt := new(v1.Int)
					unionInt.Init(unionValueType.Bytes, unionValueType.Pos)
					v1.FieldStart(builder)
					v1.FieldAddValueType(builder, v1.ValueTypeInt)
					v1.FieldEnd(builder)
					f := flatbuffers.UOffsetT(unionInt.Value())
					//v1.IntAddValue(builder, int64(f))
					fieldList = append(fieldList, f)
				} else if valueType == v1.ValueTypeStringArray {
					unionStrArray := new(v1.StringArray)
					unionStrArray.Init(unionValueType.Bytes, unionValueType.Pos)
					v1.FieldStart(builder)
					v1.FieldAddValueType(builder, v1.ValueTypeStringArray)
					v1.FieldEnd(builder)
					l := unionStrArray.ValueLength()
					var offsets []flatbuffers.UOffsetT
					for j := 0; j < l; j++ {
						v := builder.CreateString(string(unionStrArray.Value(j)))
						v1.StringArrayStart(builder)
						v1.StringArrayAddValue(builder, v)
						offset := v1.StringArrayEnd(builder)
						offsets = append(offsets, offset)
					}
					v1.StringArrayStartValueVector(builder, l)
					for o := range offsets {
						builder.PrependUOffsetT(flatbuffers.UOffsetT(o))
					}
					f := builder.EndVector(l)
					fieldList = append(fieldList, f)
				} else if valueType == v1.ValueTypeIntArray {
					unionIntArray := new(v1.IntArray)
					unionIntArray.Init(unionValueType.Bytes, unionValueType.Pos)
					v1.FieldStart(builder)
					v1.FieldAddValueType(builder, v1.ValueTypeIntArray)
					v1.FieldEnd(builder)
					l := unionIntArray.ValueLength()
					var offsets []flatbuffers.UOffsetT
					for j := 0; j < l; j++ {
						v1.IntArrayStart(builder)
						v1.IntArrayAddValue(builder, flatbuffers.UOffsetT(unionIntArray.Value(j)))
						offset := v1.StringArrayEnd(builder)
						offsets = append(offsets, offset)
					}
					v1.IntArrayStartValueVector(builder, len(offsets))
					for o := range offsets {
						builder.PrependUOffsetT(flatbuffers.UOffsetT(o))
					}
					f := builder.EndVector(len(offsets))
					fieldList = append(fieldList, f)
				}
			}
		}
	}
	v1.FieldStart(builder)
	for field := range fieldList {
		v1.FieldAddValue(builder, flatbuffers.UOffsetT(field))
	}
	v1.FieldEnd(builder)
	// Serialize EntityValue
	dataBinaryLength := 10
	v1.EntityStartDataBinaryVector(builder, dataBinaryLength)
	for i := dataBinaryLength; i >= 0; i-- {
		builder.PrependByte(byte(i))
	}
	dataBinaryP := builder.EndVector(dataBinaryLength)
	v1.EntityValueStartFieldsVector(builder, len(fieldList))
	for val := range fieldList {
		builder.PrependUOffsetT(flatbuffers.UOffsetT(val))
	}
	fieldsP := builder.EndVector(len(fieldList))
	entityId := builder.CreateString(string(entityValue.EntityId()))
	v1.EntityValueStart(builder)
	v1.EntityValueAddEntityId(builder, entityId)
	time := uint64(time.Now().UnixNano())
	v1.EntityValueAddTimestampNanoseconds(builder, time)
	v1.EntityValueAddDataBinary(builder, dataBinaryP)
	v1.EntityValueAddFields(builder, fieldsP)
	v1.EntityValueEnd(builder)
	v1.WriteEntityStart(builder)
	position := v1.WriteEntityEnd(builder)
	builder.Finish(position)

	return builder, nil
}
