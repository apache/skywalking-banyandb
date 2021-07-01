package grpc

import (
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	flatbuffers "github.com/google/flatbuffers/go"
	"time"
)

type ComponentBuilderFunc func(*flatbuffers.Builder)

type writeEntityBuilder struct {
	*flatbuffers.Builder
}

func NewEntityBuilder() *writeEntityBuilder {
	return &writeEntityBuilder{
		flatbuffers.NewBuilder(1024),
	}
}

func (b *writeEntityBuilder) BuildMetaData(group, name string) ComponentBuilderFunc {
	g, n := b.Builder.CreateString(group), b.Builder.CreateString(name)
	v1.MetadataStart(b.Builder)
	v1.MetadataAddGroup(b.Builder, g)
	v1.MetadataAddName(b.Builder, n)
	metadata := v1.MetadataEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.WriteEntityAddMetaData(b, metadata)
	}
}

func (b *writeEntityBuilder) BuildEntity(id string, binary []byte, items ...interface{}) ComponentBuilderFunc {
	entityId := b.Builder.CreateString(id)
	binaryOffset := b.Builder.CreateByteVector(binary)
	l := len(items)
	var fieldOffsets []flatbuffers.UOffsetT
	for i := 0; i < l; i++ {
		o := b.BuildField(items[i])
		fieldOffsets = append(fieldOffsets, o)
	}
	v1.EntityValueStartFieldsVector(b.Builder, len(fieldOffsets))
	for i := 0; i < len(fieldOffsets); i++ {
		b.PrependUOffsetT(fieldOffsets[i])
	}
	fields := b.EndVector(len(fieldOffsets))
	v1.EntityValueStart(b.Builder)
	v1.EntityValueAddEntityId(b.Builder, entityId)
	t := uint64(time.Now().UnixNano())
	v1.EntityValueAddTimestampNanoseconds(b.Builder, t)
	v1.EntityValueAddDataBinary(b.Builder, binaryOffset)
	v1.EntityValueAddFields(b.Builder, fields)
	entity := v1.EntityValueEnd(b.Builder)
	return func(b *flatbuffers.Builder) {
		v1.WriteEntityAddEntity(b, entity)
	}
}

func (b *writeEntityBuilder) BuildField(val interface{}) flatbuffers.UOffsetT {
	var ValueTypeOffset flatbuffers.UOffsetT
	var valType v1.ValueType
	switch v := val.(type) {
	case int:
		ValueTypeOffset = b.BuildInt(int64(v))
		valType = v1.ValueTypeInt
	case []int:
		ValueTypeOffset = b.BuildInt(convert.IntToInt64(v...)...)
		valType = v1.ValueTypeIntArray
	case int64:
		ValueTypeOffset = b.BuildInt(v)
		valType = v1.ValueTypeInt
	case []int64:
		ValueTypeOffset = b.BuildInt(v...)
		valType = v1.ValueTypeIntArray
	case string:
		ValueTypeOffset = b.BuildStrValueType(v)
		valType = v1.ValueTypeString
	case []string:
		ValueTypeOffset = b.BuildStrValueType(v...)
		valType = v1.ValueTypeStringArray
	default:
		panic("not supported values")
	}

	v1.FieldStart(b.Builder)
	v1.FieldAddValue(b.Builder, ValueTypeOffset)
	v1.FieldAddValueType(b.Builder, valType)
	return v1.FieldEnd(b.Builder)
}

func (b *writeEntityBuilder) BuildStrValueType(values ...string) flatbuffers.UOffsetT {
	var strOffsets []flatbuffers.UOffsetT
	for i := 0; i < len(values); i++ {
		strOffsets = append(strOffsets, b.CreateString(values[i]))
	}
	v1.StringArrayStartValueVector(b.Builder, len(values))
	for i := 0; i < len(strOffsets); i++ {
		b.Builder.PrependUOffsetT(strOffsets[i])
	}
	int64Arr := b.Builder.EndVector(len(values))
	v1.IntArrayStart(b.Builder)
	v1.IntArrayAddValue(b.Builder, int64Arr)
	return v1.IntArrayEnd(b.Builder)
}

func (b *writeEntityBuilder) BuildInt(values ...int64) flatbuffers.UOffsetT {
	v1.IntArrayStartValueVector(b.Builder, len(values))
	for i := 0; i < len(values); i++ {
		b.Builder.PrependInt64(values[i])
	}
	int64Arr := b.Builder.EndVector(len(values))

	v1.IntArrayStart(b.Builder)
	v1.IntArrayAddValue(b.Builder, int64Arr)
	return v1.IntArrayEnd(b.Builder)
}

func (b *writeEntityBuilder) BuildDataBinary(binary []byte) flatbuffers.UOffsetT {
	dataBinaryLength := len(binary)
	v1.EntityStartDataBinaryVector(b.Builder, dataBinaryLength)
	for i := dataBinaryLength; i >= 0; i-- {
		b.Builder.PrependByte(byte(i))
	}
	dataBinaryOffset := b.Builder.EndVector(dataBinaryLength)

	return dataBinaryOffset
}

func (b *writeEntityBuilder) Build(funcs ...ComponentBuilderFunc) *v1.WriteEntity {
	v1.WriteEntityStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	entityOffset := v1.WriteEntityEnd(b.Builder)
	b.Builder.Finish(entityOffset)

	buf := b.Bytes[b.Head():]
	return v1.GetRootAsWriteEntity(buf, 0)
}