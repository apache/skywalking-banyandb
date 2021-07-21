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
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

type ComponentBuilderFunc func(*flatbuffers.Builder)

type WriteEntityBuilder struct {
	*flatbuffers.Builder
}

func NewWriteEntityBuilder() *WriteEntityBuilder {
	return &WriteEntityBuilder{
		flatbuffers.NewBuilder(1024),
	}
}

func (b *WriteEntityBuilder) BuildMetaData(group, name string) ComponentBuilderFunc {
	g, n := b.Builder.CreateString(group), b.Builder.CreateString(name)
	v1.MetadataStart(b.Builder)
	v1.MetadataAddGroup(b.Builder, g)
	v1.MetadataAddName(b.Builder, n)
	metadata := v1.MetadataEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.WriteEntityAddMetaData(b, metadata)
	}
}

func (b *WriteEntityBuilder) BuildEntity(id string, binary []byte, items ...interface{}) ComponentBuilderFunc {
	return b.BuildEntityWithTS(id, binary, uint64(time.Now().UnixNano()), items...)
}

func (b *WriteEntityBuilder) BuildEntityWithTS(id string, binary []byte, ts uint64, items ...interface{}) ComponentBuilderFunc {
	entityID := b.Builder.CreateString(id)
	binaryOffset := b.CreateByteVector(binary)
	l := len(items)
	var fieldOffsets []flatbuffers.UOffsetT
	for i := 0; i < l; i++ {
		o := b.buildField(items[i])
		fieldOffsets = append(fieldOffsets, o)
	}
	v1.EntityStartFieldsVector(b.Builder, len(fieldOffsets))
	for i := len(fieldOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(fieldOffsets[i])
	}
	fields := b.EndVector(len(fieldOffsets))
	v1.EntityStart(b.Builder)
	v1.EntityAddEntityId(b.Builder, entityID)
	v1.EntityAddTimestampNanoseconds(b.Builder, ts)
	v1.EntityAddDataBinary(b.Builder, binaryOffset)
	v1.EntityAddFields(b.Builder, fields)
	entity := v1.EntityEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.WriteEntityAddEntity(b, entity)
	}
}

func (b *WriteEntityBuilder) buildField(val interface{}) flatbuffers.UOffsetT {
	if val == nil {
		v1.FieldStart(b.Builder)
		v1.FieldAddValueType(b.Builder, v1.ValueTypeNONE)
		return v1.FieldEnd(b.Builder)
	}
	var valueTypeOffset flatbuffers.UOffsetT
	var valType v1.ValueType
	switch v := val.(type) {
	case int:
		valueTypeOffset = b.buildInt(int64(v))
		valType = v1.ValueTypeInt
	case []int:
		valueTypeOffset = b.buildInt(convert.IntToInt64(v...)...)
		valType = v1.ValueTypeIntArray
	case int64:
		valueTypeOffset = b.buildInt(v)
		valType = v1.ValueTypeInt
	case []int64:
		valueTypeOffset = b.buildInt(v...)
		valType = v1.ValueTypeIntArray
	case string:
		valueTypeOffset = b.buildStrValueType(v)
		valType = v1.ValueTypeStr
	case []string:
		valueTypeOffset = b.buildStrValueType(v...)
		valType = v1.ValueTypeStrArray
	default:
		panic("not supported value")
	}

	v1.FieldStart(b.Builder)
	v1.FieldAddValue(b.Builder, valueTypeOffset)
	v1.FieldAddValueType(b.Builder, valType)
	return v1.FieldEnd(b.Builder)
}

func (b *WriteEntityBuilder) buildStrValueType(values ...string) flatbuffers.UOffsetT {
	strOffsets := make([]flatbuffers.UOffsetT, 0)
	for i := 0; i < len(values); i++ {
		strOffsets = append(strOffsets, b.CreateString(values[i]))
	}
	if len(values) == 1 {
		v1.StrStart(b.Builder)
		v1.StrAddValue(b.Builder, strOffsets[0])
		return v1.StrEnd(b.Builder)
	}
	v1.StrArrayStartValueVector(b.Builder, len(values))
	for i := len(strOffsets) - 1; i >= 0; i-- {
		b.Builder.PrependUOffsetT(strOffsets[i])
	}
	int64Arr := b.Builder.EndVector(len(values))
	v1.StrArrayStart(b.Builder)
	v1.StrArrayAddValue(b.Builder, int64Arr)
	return v1.StrArrayEnd(b.Builder)
}

func (b *WriteEntityBuilder) buildInt(values ...int64) flatbuffers.UOffsetT {
	if len(values) == 1 {
		v1.IntStart(b.Builder)
		v1.IntAddValue(b.Builder, values[0])
		return v1.IntEnd(b.Builder)
	}
	v1.IntArrayStartValueVector(b.Builder, len(values))
	for i := len(values) - 1; i >= 0; i-- {
		b.Builder.PrependInt64(values[i])
	}
	int64Arr := b.Builder.EndVector(len(values))

	v1.IntArrayStart(b.Builder)
	v1.IntArrayAddValue(b.Builder, int64Arr)
	return v1.IntArrayEnd(b.Builder)
}

func (b *WriteEntityBuilder) BuildWriteEntity(funcs ...ComponentBuilderFunc) (*flatbuffers.Builder, error) {
	v1.WriteEntityStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	entityOffset := v1.WriteEntityEnd(b.Builder)
	b.Builder.Finish(entityOffset)

	return b.Builder, nil
}

var binaryOpsMap = map[string]v1.BinaryOp{
	"=":          v1.BinaryOpEQ,
	"!=":         v1.BinaryOpNE,
	">":          v1.BinaryOpGT,
	">=":         v1.BinaryOpGE,
	"<":          v1.BinaryOpLT,
	"<=":         v1.BinaryOpLE,
	"having":     v1.BinaryOpHAVING,
	"not having": v1.BinaryOpNOT_HAVING,
}

type criteriaBuilder struct {
	*flatbuffers.Builder
}

func NewCriteriaBuilder() *criteriaBuilder {
	return &criteriaBuilder{
		flatbuffers.NewBuilder(1024),
	}
}

func AddLimit(limit uint32) ComponentBuilderFunc {
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddLimit(b, limit)
	}
}

func AddOffset(offset uint32) ComponentBuilderFunc {
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddOffset(b, offset)
	}
}

func (b *criteriaBuilder) BuildMetaData(group, name string) ComponentBuilderFunc {
	g, n := b.Builder.CreateString(group), b.Builder.CreateString(name)
	v1.MetadataStart(b.Builder)
	v1.MetadataAddGroup(b.Builder, g)
	v1.MetadataAddName(b.Builder, n)
	metadata := v1.MetadataEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddMetadata(b, metadata)
	}
}

func (b *criteriaBuilder) BuildTimeStampNanoSeconds(start, end time.Time) ComponentBuilderFunc {
	v1.RangeQueryStart(b.Builder)
	v1.RangeQueryAddBegin(b.Builder, uint64(start.UnixNano()))
	v1.RangeQueryAddEnd(b.Builder, uint64(end.UnixNano()))
	rangeQuery := v1.RangeQueryEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddTimestampNanoseconds(b, rangeQuery)
	}
}

func (b *criteriaBuilder) BuildOrderBy(keyName string, sort v1.Sort) ComponentBuilderFunc {
	k := b.Builder.CreateString(keyName)
	v1.QueryOrderStart(b.Builder)
	v1.QueryOrderAddKeyName(b.Builder, k)
	v1.QueryOrderAddSort(b.Builder, sort)
	orderBy := v1.QueryOrderEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddOrderBy(b, orderBy)
	}
}

func (b *criteriaBuilder) BuildProjection(keyNames ...string) ComponentBuilderFunc {
	var keyNamesOffsets []flatbuffers.UOffsetT
	for i := 0; i < len(keyNames); i++ {
		keyNamesOffsets = append(keyNamesOffsets, b.Builder.CreateString(keyNames[i]))
	}
	v1.ProjectionStartKeyNamesVector(b.Builder, len(keyNames))
	for i := len(keyNamesOffsets) - 1; i >= 0; i-- {
		b.Builder.PrependUOffsetT(keyNamesOffsets[i])
	}
	strArr := b.Builder.EndVector(len(keyNames))
	v1.ProjectionStart(b.Builder)
	v1.ProjectionAddKeyNames(b.Builder, strArr)
	projection := v1.ProjectionEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddProjection(b, projection)
	}
}

func buildIntPair(b *flatbuffers.Builder, key string, values ...int64) flatbuffers.UOffsetT {
	v1.IntPairStartValuesVector(b, len(values))
	for i := len(values) - 1; i >= 0; i-- {
		b.PrependInt64(values[i])
	}
	int64Arr := b.EndVector(len(values))

	keyOffset := b.CreateString(key)
	v1.IntPairStart(b)
	v1.IntPairAddKey(b, keyOffset)
	v1.IntPairAddValues(b, int64Arr)
	return v1.IntPairEnd(b)
}

func buildStrPair(b *flatbuffers.Builder, key string, values ...string) flatbuffers.UOffsetT {
	var strOffsets []flatbuffers.UOffsetT
	for i := 0; i < len(values); i++ {
		strOffsets = append(strOffsets, b.CreateString(values[i]))
	}
	v1.StrPairStartValuesVector(b, len(values))
	for i := len(strOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(strOffsets[i])
	}
	int64Arr := b.EndVector(len(values))

	keyOffset := b.CreateString(key)
	v1.IntPairStart(b)
	v1.IntPairAddKey(b, keyOffset)
	v1.IntPairAddValues(b, int64Arr)
	return v1.IntPairEnd(b)
}

func buildPair(b *flatbuffers.Builder, key string, value interface{}) flatbuffers.UOffsetT {
	var pairOffset flatbuffers.UOffsetT
	var pairType v1.TypedPair
	switch v := value.(type) {
	case int:
		pairOffset = buildIntPair(b, key, int64(v))
		pairType = v1.TypedPairIntPair
	case []int:
		pairOffset = buildIntPair(b, key, convert.IntToInt64(v...)...)
		pairType = v1.TypedPairIntPair
	case int8:
		pairOffset = buildIntPair(b, key, int64(v))
		pairType = v1.TypedPairIntPair
	case []int8:
		pairOffset = buildIntPair(b, key, convert.Int8ToInt64(v...)...)
		pairType = v1.TypedPairIntPair
	case int16:
		pairOffset = buildIntPair(b, key, int64(v))
		pairType = v1.TypedPairIntPair
	case []int16:
		pairOffset = buildIntPair(b, key, convert.Int16ToInt64(v...)...)
		pairType = v1.TypedPairIntPair
	case int32:
		pairOffset = buildIntPair(b, key, int64(v))
		pairType = v1.TypedPairIntPair
	case []int32:
		pairOffset = buildIntPair(b, key, convert.Int32ToInt64(v...)...)
		pairType = v1.TypedPairIntPair
	case int64:
		pairOffset = buildIntPair(b, key, v)
		pairType = v1.TypedPairIntPair
	case []int64:
		pairOffset = buildIntPair(b, key, v...)
		pairType = v1.TypedPairIntPair
	case string:
		pairOffset = buildStrPair(b, key, v)
		pairType = v1.TypedPairStrPair
	case []string:
		pairOffset = buildStrPair(b, key, v...)
		pairType = v1.TypedPairStrPair
	default:
		panic("not supported values")
	}

	v1.PairStart(b)
	v1.PairAddPair(b, pairOffset)
	v1.PairAddPairType(b, pairType)
	return v1.PairEnd(b)
}

func (b *criteriaBuilder) BuildFields(items ...interface{}) ComponentBuilderFunc {
	if len(items)%3 != 0 {
		panic("invalid items list")
	}
	l := len(items) / 3
	var pairQueryOffsets []flatbuffers.UOffsetT
	for i := 0; i < l; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		condition := buildPair(b.Builder, key.(string), values)
		v1.PairQueryStart(b.Builder)
		// add op
		v1.PairQueryAddOp(b.Builder, binaryOpsMap[op.(string)])
		// build condition
		v1.PairQueryAddCondition(b.Builder, condition)
		pairQueryOffsets = append(pairQueryOffsets, v1.PairQueryEnd(b.Builder))
	}
	v1.EntityCriteriaStartFieldsVector(b.Builder, l)
	for i := len(pairQueryOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(pairQueryOffsets[i])
	}
	fields := b.EndVector(l)
	return func(b *flatbuffers.Builder) {
		v1.EntityCriteriaAddFields(b, fields)
	}
}

func (b *criteriaBuilder) BuildEntityCriteria(funcs ...ComponentBuilderFunc) *v1.EntityCriteria {
	v1.EntityCriteriaStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	criteriaOffset := v1.EntityCriteriaEnd(b.Builder)
	b.Builder.Finish(criteriaOffset)

	buf := b.Bytes[b.Head():]
	return v1.GetRootAsEntityCriteria(buf, 0)
}

func (b *criteriaBuilder) BuildQueryEntity(funcs ...ComponentBuilderFunc) (*flatbuffers.Builder, error) {
	v1.EntityCriteriaStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	criteriaOffset := v1.EntityCriteriaEnd(b.Builder)
	b.Builder.Finish(criteriaOffset)

	return b.Builder, nil
}

type queryEntityBuilder struct {
	*flatbuffers.Builder
}

func NewQueryEntityBuilder() *queryEntityBuilder {
	return &queryEntityBuilder{
		flatbuffers.NewBuilder(1024),
	}
}

func (b *queryEntityBuilder) BuildEntityID(entityID string) ComponentBuilderFunc {
	eID := b.Builder.CreateString(entityID)
	return func(b *flatbuffers.Builder) {
		v1.EntityAddDataBinary(b, eID)
	}
}

func (b *queryEntityBuilder) BuildTimeStamp(t time.Time) ComponentBuilderFunc {
	return func(b *flatbuffers.Builder) {
		v1.EntityAddTimestampNanoseconds(b, uint64(t.UnixNano()))
	}
}

func (b *queryEntityBuilder) BuildFields(items ...interface{}) ComponentBuilderFunc {
	if len(items)%2 != 0 {
		panic("invalid fields list")
	}

	l := len(items) / 2

	var pairOffsets []flatbuffers.UOffsetT
	for i := 0; i < l; i++ {
		key, values := items[i*2+0], items[i*2+1]
		pair := buildPair(b.Builder, key.(string), values)
		pairOffsets = append(pairOffsets, pair)
	}

	v1.EntityStartFieldsVector(b.Builder, l)
	for i := len(pairOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(pairOffsets[i])
	}
	fields := b.EndVector(l)

	return func(b *flatbuffers.Builder) {
		v1.EntityAddFields(b, fields)
	}
}

func (b *queryEntityBuilder) BuildEntity(funcs ...ComponentBuilderFunc) *v1.Entity {
	v1.EntityStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	criteriaOffset := v1.EntityEnd(b.Builder)
	b.Builder.Finish(criteriaOffset)

	buf := b.Bytes[b.Head():]
	return v1.GetRootAsEntity(buf, 0)
}
