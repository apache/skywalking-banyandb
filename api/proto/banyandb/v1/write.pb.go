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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.17.3
// source: banyandb/v1/write.proto

package v1

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Str struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value string `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Str) Reset() {
	*x = Str{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Str) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Str) ProtoMessage() {}

func (x *Str) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Str.ProtoReflect.Descriptor instead.
func (*Str) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{0}
}

func (x *Str) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type Int struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value int64 `protobuf:"varint,1,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Int) Reset() {
	*x = Int{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Int) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Int) ProtoMessage() {}

func (x *Int) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Int.ProtoReflect.Descriptor instead.
func (*Int) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{1}
}

func (x *Int) GetValue() int64 {
	if x != nil {
		return x.Value
	}
	return 0
}

type StrArray struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value []string `protobuf:"bytes,1,rep,name=value,proto3" json:"value,omitempty"`
}

func (x *StrArray) Reset() {
	*x = StrArray{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StrArray) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StrArray) ProtoMessage() {}

func (x *StrArray) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StrArray.ProtoReflect.Descriptor instead.
func (*StrArray) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{2}
}

func (x *StrArray) GetValue() []string {
	if x != nil {
		return x.Value
	}
	return nil
}

type IntArray struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value []int64 `protobuf:"varint,1,rep,packed,name=value,proto3" json:"value,omitempty"`
}

func (x *IntArray) Reset() {
	*x = IntArray{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IntArray) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IntArray) ProtoMessage() {}

func (x *IntArray) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IntArray.ProtoReflect.Descriptor instead.
func (*IntArray) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{3}
}

func (x *IntArray) GetValue() []int64 {
	if x != nil {
		return x.Value
	}
	return nil
}

type Field struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to ValueType:
	//	*Field_Null
	//	*Field_Str
	//	*Field_StrArray
	//	*Field_Int
	//	*Field_IntArray
	ValueType isField_ValueType `protobuf_oneof:"value_type"`
}

func (x *Field) Reset() {
	*x = Field{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Field) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Field) ProtoMessage() {}

func (x *Field) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Field.ProtoReflect.Descriptor instead.
func (*Field) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{4}
}

func (m *Field) GetValueType() isField_ValueType {
	if m != nil {
		return m.ValueType
	}
	return nil
}

func (x *Field) GetNull() structpb.NullValue {
	if x, ok := x.GetValueType().(*Field_Null); ok {
		return x.Null
	}
	return structpb.NullValue(0)
}

func (x *Field) GetStr() *Str {
	if x, ok := x.GetValueType().(*Field_Str); ok {
		return x.Str
	}
	return nil
}

func (x *Field) GetStrArray() *StrArray {
	if x, ok := x.GetValueType().(*Field_StrArray); ok {
		return x.StrArray
	}
	return nil
}

func (x *Field) GetInt() *Int {
	if x, ok := x.GetValueType().(*Field_Int); ok {
		return x.Int
	}
	return nil
}

func (x *Field) GetIntArray() *IntArray {
	if x, ok := x.GetValueType().(*Field_IntArray); ok {
		return x.IntArray
	}
	return nil
}

type isField_ValueType interface {
	isField_ValueType()
}

type Field_Null struct {
	Null structpb.NullValue `protobuf:"varint,1,opt,name=null,proto3,enum=google.protobuf.NullValue,oneof"`
}

type Field_Str struct {
	Str *Str `protobuf:"bytes,2,opt,name=str,proto3,oneof"`
}

type Field_StrArray struct {
	StrArray *StrArray `protobuf:"bytes,3,opt,name=str_array,json=strArray,proto3,oneof"`
}

type Field_Int struct {
	Int *Int `protobuf:"bytes,4,opt,name=int,proto3,oneof"`
}

type Field_IntArray struct {
	IntArray *IntArray `protobuf:"bytes,5,opt,name=int_array,json=intArray,proto3,oneof"`
}

func (*Field_Null) isField_ValueType() {}

func (*Field_Str) isField_ValueType() {}

func (*Field_StrArray) isField_ValueType() {}

func (*Field_Int) isField_ValueType() {}

func (*Field_IntArray) isField_ValueType() {}

type EntityValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// entity_id could be span_id of a Span or segment_id of a Segment in the context of Trace
	EntityId string `protobuf:"bytes,1,opt,name=entity_id,json=entityId,proto3" json:"entity_id,omitempty"`
	// timestamp_nanoseconds is in the timeunit of nanoseconds. It represents
	// 1) either the start time of a Span/Segment,
	// 2) or the timestamp of a log
	Timestamp *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// binary representation of segments, including tags, spans...
	DataBinary []byte `protobuf:"bytes,3,opt,name=data_binary,json=dataBinary,proto3" json:"data_binary,omitempty"`
	// support all of indexed fields in the fields.
	// Pair only has value, as the value of PairValue match with the key
	// by the index rules and index rule bindings of Metadata group.
	// indexed fields of multiple entities are compression in the fields.
	Fields []*Field `protobuf:"bytes,4,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (x *EntityValue) Reset() {
	*x = EntityValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EntityValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EntityValue) ProtoMessage() {}

func (x *EntityValue) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EntityValue.ProtoReflect.Descriptor instead.
func (*EntityValue) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{5}
}

func (x *EntityValue) GetEntityId() string {
	if x != nil {
		return x.EntityId
	}
	return ""
}

func (x *EntityValue) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *EntityValue) GetDataBinary() []byte {
	if x != nil {
		return x.DataBinary
	}
	return nil
}

func (x *EntityValue) GetFields() []*Field {
	if x != nil {
		return x.Fields
	}
	return nil
}

type WriteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// the metadata is only required in the first write.
	Metadata *Metadata `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// the entity is required.
	Entity *EntityValue `protobuf:"bytes,2,opt,name=entity,proto3" json:"entity,omitempty"`
}

func (x *WriteRequest) Reset() {
	*x = WriteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteRequest) ProtoMessage() {}

func (x *WriteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteRequest.ProtoReflect.Descriptor instead.
func (*WriteRequest) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{6}
}

func (x *WriteRequest) GetMetadata() *Metadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *WriteRequest) GetEntity() *EntityValue {
	if x != nil {
		return x.Entity
	}
	return nil
}

type WriteResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *WriteResponse) Reset() {
	*x = WriteResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_write_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteResponse) ProtoMessage() {}

func (x *WriteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_write_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteResponse.ProtoReflect.Descriptor instead.
func (*WriteResponse) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_write_proto_rawDescGZIP(), []int{7}
}

var File_banyandb_v1_write_proto protoreflect.FileDescriptor

var file_banyandb_v1_write_proto_rawDesc = []byte{
	0x0a, 0x17, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x76, 0x31, 0x2f, 0x77, 0x72,
	0x69, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1a, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f,
	0x76, 0x31, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x1b, 0x0a, 0x03, 0x53, 0x74, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x1b,
	0x0a, 0x03, 0x49, 0x6e, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x20, 0x0a, 0x08, 0x53,
	0x74, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x20, 0x0a,
	0x08, 0x49, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x01, 0x20, 0x03, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22,
	0xff, 0x01, 0x0a, 0x05, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x12, 0x30, 0x0a, 0x04, 0x6e, 0x75, 0x6c,
	0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4e, 0x75, 0x6c, 0x6c, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x48, 0x00, 0x52, 0x04, 0x6e, 0x75, 0x6c, 0x6c, 0x12, 0x24, 0x0a, 0x03, 0x73,
	0x74, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x48, 0x00, 0x52, 0x03, 0x73, 0x74,
	0x72, 0x12, 0x34, 0x0a, 0x09, 0x73, 0x74, 0x72, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e,
	0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x08, 0x73,
	0x74, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x24, 0x0a, 0x03, 0x69, 0x6e, 0x74, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e,
	0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x48, 0x00, 0x52, 0x03, 0x69, 0x6e, 0x74, 0x12, 0x34, 0x0a,
	0x09, 0x69, 0x6e, 0x74, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x15, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x49,
	0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x08, 0x69, 0x6e, 0x74, 0x41, 0x72,
	0x72, 0x61, 0x79, 0x42, 0x0c, 0x0a, 0x0a, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x22, 0xb1, 0x01, 0x0a, 0x0b, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x12, 0x1b, 0x0a, 0x09, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x5f, 0x69, 0x64, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x49, 0x64, 0x12, 0x38,
	0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1f, 0x0a, 0x0b, 0x64, 0x61, 0x74, 0x61,
	0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0a, 0x64,
	0x61, 0x74, 0x61, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x12, 0x2a, 0x0a, 0x06, 0x66, 0x69, 0x65,
	0x6c, 0x64, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x62, 0x61, 0x6e, 0x79,
	0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x52, 0x06, 0x66,
	0x69, 0x65, 0x6c, 0x64, 0x73, 0x22, 0x73, 0x0a, 0x0c, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x31, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74,
	0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e,
	0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x08,
	0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x30, 0x0a, 0x06, 0x65, 0x6e, 0x74, 0x69,
	0x74, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x52, 0x06, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x22, 0x0f, 0x0a, 0x0d, 0x57, 0x72,
	0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x60, 0x0a, 0x1e, 0x6f,
	0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c,
	0x6b, 0x69, 0x6e, 0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x5a, 0x3e, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65,
	0x2f, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2d, 0x62, 0x61, 0x6e, 0x79,
	0x61, 0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x76, 0x31, 0x3b, 0x76, 0x31, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_v1_write_proto_rawDescOnce sync.Once
	file_banyandb_v1_write_proto_rawDescData = file_banyandb_v1_write_proto_rawDesc
)

func file_banyandb_v1_write_proto_rawDescGZIP() []byte {
	file_banyandb_v1_write_proto_rawDescOnce.Do(func() {
		file_banyandb_v1_write_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_v1_write_proto_rawDescData)
	})
	return file_banyandb_v1_write_proto_rawDescData
}

var file_banyandb_v1_write_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_banyandb_v1_write_proto_goTypes = []interface{}{
	(*Str)(nil),                   // 0: banyandb.v1.Str
	(*Int)(nil),                   // 1: banyandb.v1.Int
	(*StrArray)(nil),              // 2: banyandb.v1.StrArray
	(*IntArray)(nil),              // 3: banyandb.v1.IntArray
	(*Field)(nil),                 // 4: banyandb.v1.Field
	(*EntityValue)(nil),           // 5: banyandb.v1.EntityValue
	(*WriteRequest)(nil),          // 6: banyandb.v1.WriteRequest
	(*WriteResponse)(nil),         // 7: banyandb.v1.WriteResponse
	(structpb.NullValue)(0),       // 8: google.protobuf.NullValue
	(*timestamppb.Timestamp)(nil), // 9: google.protobuf.Timestamp
	(*Metadata)(nil),              // 10: banyandb.v1.Metadata
}
var file_banyandb_v1_write_proto_depIdxs = []int32{
	8,  // 0: banyandb.v1.Field.null:type_name -> google.protobuf.NullValue
	0,  // 1: banyandb.v1.Field.str:type_name -> banyandb.v1.Str
	2,  // 2: banyandb.v1.Field.str_array:type_name -> banyandb.v1.StrArray
	1,  // 3: banyandb.v1.Field.int:type_name -> banyandb.v1.Int
	3,  // 4: banyandb.v1.Field.int_array:type_name -> banyandb.v1.IntArray
	9,  // 5: banyandb.v1.EntityValue.timestamp:type_name -> google.protobuf.Timestamp
	4,  // 6: banyandb.v1.EntityValue.fields:type_name -> banyandb.v1.Field
	10, // 7: banyandb.v1.WriteRequest.metadata:type_name -> banyandb.v1.Metadata
	5,  // 8: banyandb.v1.WriteRequest.entity:type_name -> banyandb.v1.EntityValue
	9,  // [9:9] is the sub-list for method output_type
	9,  // [9:9] is the sub-list for method input_type
	9,  // [9:9] is the sub-list for extension type_name
	9,  // [9:9] is the sub-list for extension extendee
	0,  // [0:9] is the sub-list for field type_name
}

func init() { file_banyandb_v1_write_proto_init() }
func file_banyandb_v1_write_proto_init() {
	if File_banyandb_v1_write_proto != nil {
		return
	}
	file_banyandb_v1_database_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_banyandb_v1_write_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Str); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Int); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StrArray); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IntArray); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Field); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EntityValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_banyandb_v1_write_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_banyandb_v1_write_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*Field_Null)(nil),
		(*Field_Str)(nil),
		(*Field_StrArray)(nil),
		(*Field_Int)(nil),
		(*Field_IntArray)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_banyandb_v1_write_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_v1_write_proto_goTypes,
		DependencyIndexes: file_banyandb_v1_write_proto_depIdxs,
		MessageInfos:      file_banyandb_v1_write_proto_msgTypes,
	}.Build()
	File_banyandb_v1_write_proto = out.File
	file_banyandb_v1_write_proto_rawDesc = nil
	file_banyandb_v1_write_proto_goTypes = nil
	file_banyandb_v1_write_proto_depIdxs = nil
}
