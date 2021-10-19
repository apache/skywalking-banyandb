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
// 	protoc        v3.18.1
// source: banyandb/model/v1/common.proto

package v1

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
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
		mi := &file_banyandb_model_v1_common_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Str) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Str) ProtoMessage() {}

func (x *Str) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[0]
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
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{0}
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
		mi := &file_banyandb_model_v1_common_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Int) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Int) ProtoMessage() {}

func (x *Int) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[1]
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
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{1}
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
		mi := &file_banyandb_model_v1_common_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StrArray) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StrArray) ProtoMessage() {}

func (x *StrArray) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[2]
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
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{2}
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
		mi := &file_banyandb_model_v1_common_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IntArray) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IntArray) ProtoMessage() {}

func (x *IntArray) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[3]
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
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{3}
}

func (x *IntArray) GetValue() []int64 {
	if x != nil {
		return x.Value
	}
	return nil
}

type TagValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Value:
	//	*TagValue_Null
	//	*TagValue_Str
	//	*TagValue_StrArray
	//	*TagValue_Int
	//	*TagValue_IntArray
	//	*TagValue_BinaryData
	Value isTagValue_Value `protobuf_oneof:"value"`
}

func (x *TagValue) Reset() {
	*x = TagValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_common_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TagValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagValue) ProtoMessage() {}

func (x *TagValue) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagValue.ProtoReflect.Descriptor instead.
func (*TagValue) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{4}
}

func (m *TagValue) GetValue() isTagValue_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (x *TagValue) GetNull() structpb.NullValue {
	if x, ok := x.GetValue().(*TagValue_Null); ok {
		return x.Null
	}
	return structpb.NullValue(0)
}

func (x *TagValue) GetStr() *Str {
	if x, ok := x.GetValue().(*TagValue_Str); ok {
		return x.Str
	}
	return nil
}

func (x *TagValue) GetStrArray() *StrArray {
	if x, ok := x.GetValue().(*TagValue_StrArray); ok {
		return x.StrArray
	}
	return nil
}

func (x *TagValue) GetInt() *Int {
	if x, ok := x.GetValue().(*TagValue_Int); ok {
		return x.Int
	}
	return nil
}

func (x *TagValue) GetIntArray() *IntArray {
	if x, ok := x.GetValue().(*TagValue_IntArray); ok {
		return x.IntArray
	}
	return nil
}

func (x *TagValue) GetBinaryData() []byte {
	if x, ok := x.GetValue().(*TagValue_BinaryData); ok {
		return x.BinaryData
	}
	return nil
}

type isTagValue_Value interface {
	isTagValue_Value()
}

type TagValue_Null struct {
	Null structpb.NullValue `protobuf:"varint,1,opt,name=null,proto3,enum=google.protobuf.NullValue,oneof"`
}

type TagValue_Str struct {
	Str *Str `protobuf:"bytes,2,opt,name=str,proto3,oneof"`
}

type TagValue_StrArray struct {
	StrArray *StrArray `protobuf:"bytes,3,opt,name=str_array,json=strArray,proto3,oneof"`
}

type TagValue_Int struct {
	Int *Int `protobuf:"bytes,4,opt,name=int,proto3,oneof"`
}

type TagValue_IntArray struct {
	IntArray *IntArray `protobuf:"bytes,5,opt,name=int_array,json=intArray,proto3,oneof"`
}

type TagValue_BinaryData struct {
	BinaryData []byte `protobuf:"bytes,6,opt,name=binary_data,json=binaryData,proto3,oneof"`
}

func (*TagValue_Null) isTagValue_Value() {}

func (*TagValue_Str) isTagValue_Value() {}

func (*TagValue_StrArray) isTagValue_Value() {}

func (*TagValue_Int) isTagValue_Value() {}

func (*TagValue_IntArray) isTagValue_Value() {}

func (*TagValue_BinaryData) isTagValue_Value() {}

type TagFamilyForWrite struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tags []*TagValue `protobuf:"bytes,1,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *TagFamilyForWrite) Reset() {
	*x = TagFamilyForWrite{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_common_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TagFamilyForWrite) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagFamilyForWrite) ProtoMessage() {}

func (x *TagFamilyForWrite) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagFamilyForWrite.ProtoReflect.Descriptor instead.
func (*TagFamilyForWrite) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{5}
}

func (x *TagFamilyForWrite) GetTags() []*TagValue {
	if x != nil {
		return x.Tags
	}
	return nil
}

type FieldValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Value:
	//	*FieldValue_Null
	//	*FieldValue_Str
	//	*FieldValue_Int
	//	*FieldValue_BinaryData
	Value isFieldValue_Value `protobuf_oneof:"value"`
}

func (x *FieldValue) Reset() {
	*x = FieldValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_common_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FieldValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FieldValue) ProtoMessage() {}

func (x *FieldValue) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_common_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FieldValue.ProtoReflect.Descriptor instead.
func (*FieldValue) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_common_proto_rawDescGZIP(), []int{6}
}

func (m *FieldValue) GetValue() isFieldValue_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (x *FieldValue) GetNull() structpb.NullValue {
	if x, ok := x.GetValue().(*FieldValue_Null); ok {
		return x.Null
	}
	return structpb.NullValue(0)
}

func (x *FieldValue) GetStr() *Str {
	if x, ok := x.GetValue().(*FieldValue_Str); ok {
		return x.Str
	}
	return nil
}

func (x *FieldValue) GetInt() *Int {
	if x, ok := x.GetValue().(*FieldValue_Int); ok {
		return x.Int
	}
	return nil
}

func (x *FieldValue) GetBinaryData() []byte {
	if x, ok := x.GetValue().(*FieldValue_BinaryData); ok {
		return x.BinaryData
	}
	return nil
}

type isFieldValue_Value interface {
	isFieldValue_Value()
}

type FieldValue_Null struct {
	Null structpb.NullValue `protobuf:"varint,1,opt,name=null,proto3,enum=google.protobuf.NullValue,oneof"`
}

type FieldValue_Str struct {
	Str *Str `protobuf:"bytes,2,opt,name=str,proto3,oneof"`
}

type FieldValue_Int struct {
	Int *Int `protobuf:"bytes,4,opt,name=int,proto3,oneof"`
}

type FieldValue_BinaryData struct {
	BinaryData []byte `protobuf:"bytes,6,opt,name=binary_data,json=binaryData,proto3,oneof"`
}

func (*FieldValue_Null) isFieldValue_Value() {}

func (*FieldValue_Str) isFieldValue_Value() {}

func (*FieldValue_Int) isFieldValue_Value() {}

func (*FieldValue_BinaryData) isFieldValue_Value() {}

var File_banyandb_model_v1_common_proto protoreflect.FileDescriptor

var file_banyandb_model_v1_common_proto_rawDesc = []byte{
	0x0a, 0x1e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x11, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2e, 0x76, 0x31, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x1b, 0x0a, 0x03, 0x53, 0x74, 0x72, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x1b,
	0x0a, 0x03, 0x49, 0x6e, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x20, 0x0a, 0x08, 0x53,
	0x74, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x20, 0x0a,
	0x08, 0x49, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x01, 0x20, 0x03, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22,
	0xb8, 0x02, 0x0a, 0x08, 0x54, 0x61, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x30, 0x0a, 0x04,
	0x6e, 0x75, 0x6c, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4e, 0x75, 0x6c,
	0x6c, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x48, 0x00, 0x52, 0x04, 0x6e, 0x75, 0x6c, 0x6c, 0x12, 0x2a,
	0x0a, 0x03, 0x73, 0x74, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e,
	0x53, 0x74, 0x72, 0x48, 0x00, 0x52, 0x03, 0x73, 0x74, 0x72, 0x12, 0x3a, 0x0a, 0x09, 0x73, 0x74,
	0x72, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e,
	0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76,
	0x31, 0x2e, 0x53, 0x74, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x08, 0x73, 0x74,
	0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x2a, 0x0a, 0x03, 0x69, 0x6e, 0x74, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d,
	0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x48, 0x00, 0x52, 0x03, 0x69,
	0x6e, 0x74, 0x12, 0x3a, 0x0a, 0x09, 0x69, 0x6e, 0x74, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62,
	0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x41, 0x72, 0x72,
	0x61, 0x79, 0x48, 0x00, 0x52, 0x08, 0x69, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x21,
	0x0a, 0x0b, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x5f, 0x64, 0x61, 0x74, 0x61, 0x18, 0x06, 0x20,
	0x01, 0x28, 0x0c, 0x48, 0x00, 0x52, 0x0a, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x44, 0x61, 0x74,
	0x61, 0x42, 0x07, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x44, 0x0a, 0x11, 0x54, 0x61,
	0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x46, 0x6f, 0x72, 0x57, 0x72, 0x69, 0x74, 0x65, 0x12,
	0x2f, 0x0a, 0x04, 0x74, 0x61, 0x67, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1b, 0x2e,
	0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76,
	0x31, 0x2e, 0x54, 0x61, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73,
	0x22, 0xc2, 0x01, 0x0a, 0x0a, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12,
	0x30, 0x0a, 0x04, 0x6e, 0x75, 0x6c, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x4e, 0x75, 0x6c, 0x6c, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x48, 0x00, 0x52, 0x04, 0x6e, 0x75, 0x6c,
	0x6c, 0x12, 0x2a, 0x0a, 0x03, 0x73, 0x74, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16,
	0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x48, 0x00, 0x52, 0x03, 0x73, 0x74, 0x72, 0x12, 0x2a, 0x0a,
	0x03, 0x69, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x49,
	0x6e, 0x74, 0x48, 0x00, 0x52, 0x03, 0x69, 0x6e, 0x74, 0x12, 0x21, 0x0a, 0x0b, 0x62, 0x69, 0x6e,
	0x61, 0x72, 0x79, 0x5f, 0x64, 0x61, 0x74, 0x61, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x00,
	0x52, 0x0a, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x44, 0x61, 0x74, 0x61, 0x42, 0x07, 0x0a, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x6c, 0x0a, 0x27, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61,
	0x63, 0x68, 0x65, 0x2e, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2e, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31,
	0x5a, 0x41, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x70, 0x61,
	0x63, 0x68, 0x65, 0x2f, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2d, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2f, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_model_v1_common_proto_rawDescOnce sync.Once
	file_banyandb_model_v1_common_proto_rawDescData = file_banyandb_model_v1_common_proto_rawDesc
)

func file_banyandb_model_v1_common_proto_rawDescGZIP() []byte {
	file_banyandb_model_v1_common_proto_rawDescOnce.Do(func() {
		file_banyandb_model_v1_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_model_v1_common_proto_rawDescData)
	})
	return file_banyandb_model_v1_common_proto_rawDescData
}

var file_banyandb_model_v1_common_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_banyandb_model_v1_common_proto_goTypes = []interface{}{
	(*Str)(nil),               // 0: banyandb.model.v1.Str
	(*Int)(nil),               // 1: banyandb.model.v1.Int
	(*StrArray)(nil),          // 2: banyandb.model.v1.StrArray
	(*IntArray)(nil),          // 3: banyandb.model.v1.IntArray
	(*TagValue)(nil),          // 4: banyandb.model.v1.TagValue
	(*TagFamilyForWrite)(nil), // 5: banyandb.model.v1.TagFamilyForWrite
	(*FieldValue)(nil),        // 6: banyandb.model.v1.FieldValue
	(structpb.NullValue)(0),   // 7: google.protobuf.NullValue
}
var file_banyandb_model_v1_common_proto_depIdxs = []int32{
	7, // 0: banyandb.model.v1.TagValue.null:type_name -> google.protobuf.NullValue
	0, // 1: banyandb.model.v1.TagValue.str:type_name -> banyandb.model.v1.Str
	2, // 2: banyandb.model.v1.TagValue.str_array:type_name -> banyandb.model.v1.StrArray
	1, // 3: banyandb.model.v1.TagValue.int:type_name -> banyandb.model.v1.Int
	3, // 4: banyandb.model.v1.TagValue.int_array:type_name -> banyandb.model.v1.IntArray
	4, // 5: banyandb.model.v1.TagFamilyForWrite.tags:type_name -> banyandb.model.v1.TagValue
	7, // 6: banyandb.model.v1.FieldValue.null:type_name -> google.protobuf.NullValue
	0, // 7: banyandb.model.v1.FieldValue.str:type_name -> banyandb.model.v1.Str
	1, // 8: banyandb.model.v1.FieldValue.int:type_name -> banyandb.model.v1.Int
	9, // [9:9] is the sub-list for method output_type
	9, // [9:9] is the sub-list for method input_type
	9, // [9:9] is the sub-list for extension type_name
	9, // [9:9] is the sub-list for extension extendee
	0, // [0:9] is the sub-list for field type_name
}

func init() { file_banyandb_model_v1_common_proto_init() }
func file_banyandb_model_v1_common_proto_init() {
	if File_banyandb_model_v1_common_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_banyandb_model_v1_common_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_model_v1_common_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_model_v1_common_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_model_v1_common_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_model_v1_common_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TagValue); i {
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
		file_banyandb_model_v1_common_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TagFamilyForWrite); i {
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
		file_banyandb_model_v1_common_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FieldValue); i {
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
	file_banyandb_model_v1_common_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*TagValue_Null)(nil),
		(*TagValue_Str)(nil),
		(*TagValue_StrArray)(nil),
		(*TagValue_Int)(nil),
		(*TagValue_IntArray)(nil),
		(*TagValue_BinaryData)(nil),
	}
	file_banyandb_model_v1_common_proto_msgTypes[6].OneofWrappers = []interface{}{
		(*FieldValue_Null)(nil),
		(*FieldValue_Str)(nil),
		(*FieldValue_Int)(nil),
		(*FieldValue_BinaryData)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_banyandb_model_v1_common_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_model_v1_common_proto_goTypes,
		DependencyIndexes: file_banyandb_model_v1_common_proto_depIdxs,
		MessageInfos:      file_banyandb_model_v1_common_proto_msgTypes,
	}.Build()
	File_banyandb_model_v1_common_proto = out.File
	file_banyandb_model_v1_common_proto_rawDesc = nil
	file_banyandb_model_v1_common_proto_goTypes = nil
	file_banyandb_model_v1_common_proto_depIdxs = nil
}
