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
// source: banyandb/model/v1/write.proto

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
		mi := &file_banyandb_model_v1_write_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Field) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Field) ProtoMessage() {}

func (x *Field) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_write_proto_msgTypes[0]
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
	return file_banyandb_model_v1_write_proto_rawDescGZIP(), []int{0}
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

var File_banyandb_model_v1_write_proto protoreflect.FileDescriptor

var file_banyandb_model_v1_write_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2f, 0x76, 0x31, 0x2f, 0x77, 0x72, 0x69, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x11, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x76, 0x31, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x1e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x97, 0x02, 0x0a, 0x05, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x12, 0x30, 0x0a, 0x04, 0x6e, 0x75,
	0x6c, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x4e, 0x75, 0x6c, 0x6c, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x48, 0x00, 0x52, 0x04, 0x6e, 0x75, 0x6c, 0x6c, 0x12, 0x2a, 0x0a, 0x03,
	0x73, 0x74, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79,
	0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74,
	0x72, 0x48, 0x00, 0x52, 0x03, 0x73, 0x74, 0x72, 0x12, 0x3a, 0x0a, 0x09, 0x73, 0x74, 0x72, 0x5f,
	0x61, 0x72, 0x72, 0x61, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e,
	0x53, 0x74, 0x72, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x08, 0x73, 0x74, 0x72, 0x41,
	0x72, 0x72, 0x61, 0x79, 0x12, 0x2a, 0x0a, 0x03, 0x69, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64,
	0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x48, 0x00, 0x52, 0x03, 0x69, 0x6e, 0x74,
	0x12, 0x3a, 0x0a, 0x09, 0x69, 0x6e, 0x74, 0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d,
	0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79,
	0x48, 0x00, 0x52, 0x08, 0x69, 0x6e, 0x74, 0x41, 0x72, 0x72, 0x61, 0x79, 0x42, 0x0c, 0x0a, 0x0a,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x42, 0x6c, 0x0a, 0x27, 0x6f, 0x72,
	0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b,
	0x69, 0x6e, 0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64,
	0x65, 0x6c, 0x2e, 0x76, 0x31, 0x5a, 0x41, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2f, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b,
	0x69, 0x6e, 0x67, 0x2d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f,
	0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2f, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_model_v1_write_proto_rawDescOnce sync.Once
	file_banyandb_model_v1_write_proto_rawDescData = file_banyandb_model_v1_write_proto_rawDesc
)

func file_banyandb_model_v1_write_proto_rawDescGZIP() []byte {
	file_banyandb_model_v1_write_proto_rawDescOnce.Do(func() {
		file_banyandb_model_v1_write_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_model_v1_write_proto_rawDescData)
	})
	return file_banyandb_model_v1_write_proto_rawDescData
}

var file_banyandb_model_v1_write_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_banyandb_model_v1_write_proto_goTypes = []interface{}{
	(*Field)(nil),           // 0: banyandb.model.v1.Field
	(structpb.NullValue)(0), // 1: google.protobuf.NullValue
	(*Str)(nil),             // 2: banyandb.model.v1.Str
	(*StrArray)(nil),        // 3: banyandb.model.v1.StrArray
	(*Int)(nil),             // 4: banyandb.model.v1.Int
	(*IntArray)(nil),        // 5: banyandb.model.v1.IntArray
}
var file_banyandb_model_v1_write_proto_depIdxs = []int32{
	1, // 0: banyandb.model.v1.Field.null:type_name -> google.protobuf.NullValue
	2, // 1: banyandb.model.v1.Field.str:type_name -> banyandb.model.v1.Str
	3, // 2: banyandb.model.v1.Field.str_array:type_name -> banyandb.model.v1.StrArray
	4, // 3: banyandb.model.v1.Field.int:type_name -> banyandb.model.v1.Int
	5, // 4: banyandb.model.v1.Field.int_array:type_name -> banyandb.model.v1.IntArray
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_banyandb_model_v1_write_proto_init() }
func file_banyandb_model_v1_write_proto_init() {
	if File_banyandb_model_v1_write_proto != nil {
		return
	}
	file_banyandb_model_v1_common_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_banyandb_model_v1_write_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
	}
	file_banyandb_model_v1_write_proto_msgTypes[0].OneofWrappers = []interface{}{
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
			RawDescriptor: file_banyandb_model_v1_write_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_model_v1_write_proto_goTypes,
		DependencyIndexes: file_banyandb_model_v1_write_proto_depIdxs,
		MessageInfos:      file_banyandb_model_v1_write_proto_msgTypes,
	}.Build()
	File_banyandb_model_v1_write_proto = out.File
	file_banyandb_model_v1_write_proto_rawDesc = nil
	file_banyandb_model_v1_write_proto_goTypes = nil
	file_banyandb_model_v1_write_proto_depIdxs = nil
}
