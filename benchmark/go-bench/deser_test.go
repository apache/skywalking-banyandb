package go_bench

import (
	"fmt"
	flatbuffers "github.com/google/flatbuffers/go"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/fb"
)

type seriesEntity struct {
	seriesID string
	entity   entity
}

type entity struct {
	id     string
	binary []byte
	ts     uint64
	items  []interface{}
}

var se = seriesEntity{
	seriesID: "webapp_10.0.0.1",
	entity: entity{
		id:     "1231.dfd.123123ssf",
		binary: []byte{12},
		ts:     uint64(time.Now().UnixNano()),
		items:  []interface{}{"trace_id-xxfff.111323", 0, "webapp_id", "10.0.0.1_id", "/home_id", "webapp", "10.0.0.1", "/home", 300, 1622933202000000000}},
}

func Benchmark_Deser_Flatbuffers(b *testing.B) {
	web := fb.NewWriteEntityBuilder()
	items := make([]fb.ComponentBuilderFunc, 0, 2)
	items = append(items, web.BuildMetaData("default", "sw"))
	timestamp := se.entity.ts
	items = append(items, web.BuildEntityWithTS(se.entity.id, se.entity.binary, timestamp, se.entity.items...))
	builder, err := web.BuildWriteEntity(
		items...,
	)
	assert.NoError(b, err)
	data := builder.FinishedBytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		we := v1.GetRootAsWriteEntity(data, 0)
		metadata := we.MetaData(nil)
		_, _ = string(metadata.Group()), string(metadata.Name())
		e := we.Entity(nil)
		_ = e.TimestampNanoseconds()
		_ = e.DataBinaryBytes()
		for j := 0; j < e.FieldsLength(); j++ {
			var f v1.Field
			if e.Fields(&f, j) {
				unionValueType := new(flatbuffers.Table)
				_ = f.Value(unionValueType)
				if f.ValueType() == v1.ValueTypeStr {
					strVal := new(v1.Str)
					strVal.Init(unionValueType.Bytes, unionValueType.Pos)
					_ = string(strVal.Value())
				} else if f.ValueType() == v1.ValueTypeInt {
					intVal := new(v1.Int)
					intVal.Init(unionValueType.Bytes, unionValueType.Pos)
					_ = intVal.Value()
				} else {
					panic("should not reach here")
				}
			}
		}
	}
}

func Benchmark_Deser_Protobuf(b *testing.B) {
	metadata := &Metadata{Name: "sw", Group: "default"}
	entityVal := &EntityValue{
		EntityId:             se.entity.id,
		TimestampNanoseconds: se.entity.ts,
		DataBinary:           se.entity.binary,
		Fields:               buildFields(se.entity.items...),
	}
	writeEntity := &WriteEntity{
		MetaData: metadata,
		Entity:   entityVal,
	}
	data, _ := proto.Marshal(writeEntity)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		we := &WriteEntity{}
		_ = proto.Unmarshal(data, we)
	}
}

func buildFields(items ...interface{}) []*Field {
	fields := make([]*Field, len(items))
	for i := 0; i < len(items); i++ {
		fields[i] = buildField(items[i])
	}
	return fields
}

func buildField(item interface{}) *Field {
	switch v := item.(type) {
	case string:
		return &Field{ValueType: &Field_Str{&Str{Value: v}}}
	case int32:
		return &Field{ValueType: &Field_Int{&Int{Value: int64(v)}}}
	case int:
		return &Field{ValueType: &Field_Int{&Int{Value: int64(v)}}}
	case int64:
		return &Field{ValueType: &Field_Int{&Int{Value: v}}}
	case int16:
		return &Field{ValueType: &Field_Int{&Int{Value: int64(v)}}}
	case int8:
		return &Field{ValueType: &Field_Int{&Int{Value: int64(v)}}}
	default:
		fmt.Println("type=", item)
		panic("not supported type")
	}
}
