package clientutil

import (
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var binaryOpsMap = map[string]apiv1.BinaryOp{
	"=":          apiv1.BinaryOpEQ,
	"!=":         apiv1.BinaryOpNE,
	">":          apiv1.BinaryOpGT,
	">=":         apiv1.BinaryOpGE,
	"<":          apiv1.BinaryOpLT,
	"<=":         apiv1.BinaryOpLE,
	"having":     apiv1.BinaryOpHAVING,
	"not having": apiv1.BinaryOpNOT_HAVING,
}

type ComponentBuilderFunc func(*flatbuffers.Builder)

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
		apiv1.EntityCriteriaAddLimit(b, limit)
	}
}

func AddOffset(offset uint32) ComponentBuilderFunc {
	return func(b *flatbuffers.Builder) {
		apiv1.EntityCriteriaAddOffset(b, offset)
	}
}

func (b *criteriaBuilder) BuildMetaData(group, name string) ComponentBuilderFunc {
	g, n := b.CreateString(group), b.CreateString(name)
	apiv1.MetadataStart(b.Builder)
	apiv1.MetadataAddGroup(b.Builder, g)
	apiv1.MetadataAddName(b.Builder, n)
	metadata := apiv1.MetadataEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		apiv1.EntityCriteriaAddMetatdata(b, metadata)
	}
}

func (b *criteriaBuilder) BuildTimeStampNanoSeconds(start, end time.Time) ComponentBuilderFunc {
	apiv1.RangeQueryStart(b.Builder)
	apiv1.RangeQueryAddBegin(b.Builder, uint64(start.UnixNano()))
	apiv1.RangeQueryAddEnd(b.Builder, uint64(end.UnixNano()))
	rangeQuery := apiv1.RangeQueryEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		apiv1.EntityCriteriaAddTimestampNanoseconds(b, rangeQuery)
	}
}

func (b *criteriaBuilder) BuildOrderBy(keyName string, sort apiv1.Sort) ComponentBuilderFunc {
	k := b.Builder.CreateString(keyName)
	apiv1.QueryOrderStart(b.Builder)
	apiv1.QueryOrderAddKeyName(b.Builder, k)
	apiv1.QueryOrderAddSort(b.Builder, sort)
	orderBy := apiv1.QueryOrderEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		apiv1.EntityCriteriaAddOrderBy(b, orderBy)
	}
}

func (b *criteriaBuilder) BuildProjection(keyNames ...string) ComponentBuilderFunc {
	var keyNamesOffsets []flatbuffers.UOffsetT
	for i := 0; i < len(keyNames); i++ {
		keyNamesOffsets = append(keyNamesOffsets, b.Builder.CreateString(keyNames[i]))
	}
	apiv1.ProjectionStartKeyNamesVector(b.Builder, len(keyNames))
	for i := 0; i < len(keyNamesOffsets); i++ {
		b.Builder.PrependUOffsetT(keyNamesOffsets[i])
	}
	strArr := b.Builder.EndVector(len(keyNames))
	apiv1.ProjectionStart(b.Builder)
	apiv1.ProjectionAddKeyNames(b.Builder, strArr)
	projection := apiv1.ProjectionEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		apiv1.EntityCriteriaAddProjection(b, projection)
	}
}

func (b *criteriaBuilder) buildIntPair(key string, values ...int64) flatbuffers.UOffsetT {
	apiv1.IntPairStartValuesVector(b.Builder, len(values))
	for i := 0; i < len(values); i++ {
		b.Builder.PrependInt64(values[i])
	}
	int64Arr := b.Builder.EndVector(len(values))

	keyOffset := b.CreateString(key)
	apiv1.IntPairStart(b.Builder)
	apiv1.IntPairAddKey(b.Builder, keyOffset)
	apiv1.IntPairAddValues(b.Builder, int64Arr)
	return apiv1.IntPairEnd(b.Builder)
}

func (b *criteriaBuilder) buildStrPair(key string, values ...string) flatbuffers.UOffsetT {
	var strOffsets []flatbuffers.UOffsetT
	for i := 0; i < len(values); i++ {
		strOffsets = append(strOffsets, b.CreateString(values[i]))
	}
	apiv1.StrPairStartValuesVector(b.Builder, len(values))
	for i := 0; i < len(strOffsets); i++ {
		b.Builder.PrependUOffsetT(strOffsets[i])
	}
	int64Arr := b.Builder.EndVector(len(values))

	keyOffset := b.CreateString(key)
	apiv1.IntPairStart(b.Builder)
	apiv1.IntPairAddKey(b.Builder, keyOffset)
	apiv1.IntPairAddValues(b.Builder, int64Arr)
	return apiv1.IntPairEnd(b.Builder)
}

func (b *criteriaBuilder) buildCondition(key string, value interface{}) flatbuffers.UOffsetT {
	var pairOffset flatbuffers.UOffsetT
	var pairType apiv1.TypedPair
	switch v := value.(type) {
	case int:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case []int:
		pairOffset = b.buildIntPair(key, convert.IntToInt64(v...)...)
		pairType = apiv1.TypedPairIntPair
	case int8:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case []int8:
		pairOffset = b.buildIntPair(key, convert.Int8ToInt64(v...)...)
		pairType = apiv1.TypedPairIntPair
	case int16:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case []int16:
		pairOffset = b.buildIntPair(key, convert.Int16ToInt64(v...)...)
		pairType = apiv1.TypedPairIntPair
	case int32:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case []int32:
		pairOffset = b.buildIntPair(key, convert.Int32ToInt64(v...)...)
		pairType = apiv1.TypedPairIntPair
	case int64:
		pairOffset = b.buildIntPair(key, v)
		pairType = apiv1.TypedPairIntPair
	case []int64:
		pairOffset = b.buildIntPair(key, v...)
		pairType = apiv1.TypedPairIntPair
	case string:
		pairOffset = b.buildStrPair(key, v)
		pairType = apiv1.TypedPairStrPair
	case []string:
		pairOffset = b.buildStrPair(key, v...)
		pairType = apiv1.TypedPairStrPair
	default:
		panic("not supported values")
	}

	apiv1.PairStart(b.Builder)
	apiv1.PairAddPair(b.Builder, pairOffset)
	apiv1.PairAddPairType(b.Builder, pairType)
	return apiv1.PairEnd(b.Builder)
}

func (b *criteriaBuilder) BuildFields(items ...interface{}) ComponentBuilderFunc {
	if len(items)%3 != 0 {
		panic("invalid items list")
	}
	l := len(items) / 3
	var pairQueryOffsets []flatbuffers.UOffsetT
	for i := 0; i < l; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		condition := b.buildCondition(key.(string), values)
		apiv1.PairQueryStart(b.Builder)
		// add op
		apiv1.PairQueryAddOp(b.Builder, binaryOpsMap[op.(string)])
		// build condition
		apiv1.PairQueryAddCondition(b.Builder, condition)
		pairQueryOffsets = append(pairQueryOffsets, apiv1.PairQueryEnd(b.Builder))
	}
	apiv1.EntityCriteriaStartFieldsVector(b.Builder, l)
	for i := 0; i < len(pairQueryOffsets); i++ {
		b.PrependUOffsetT(pairQueryOffsets[i])
	}
	fields := b.EndVector(l)
	return func(b *flatbuffers.Builder) {
		apiv1.EntityCriteriaAddFields(b, fields)
	}
}

func (b *criteriaBuilder) Build(funcs ...ComponentBuilderFunc) *apiv1.EntityCriteria {
	apiv1.EntityCriteriaStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	criteriaOffset := apiv1.EntityCriteriaEnd(b.Builder)
	b.Builder.Finish(criteriaOffset)

	buf := b.Bytes[b.Head():]
	return apiv1.GetRootAsEntityCriteria(buf, 0)
}
