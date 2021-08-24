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
// source: banyandb/common/v2/common.proto

package v2

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Catalog int32

const (
	Catalog_CATALOG_UNSPECIFIED Catalog = 0
	Catalog_CATALOG_STREAM      Catalog = 1
	Catalog_CATALOG_MEASURE     Catalog = 2
)

// Enum value maps for Catalog.
var (
	Catalog_name = map[int32]string{
		0: "CATALOG_UNSPECIFIED",
		1: "CATALOG_STREAM",
		2: "CATALOG_MEASURE",
	}
	Catalog_value = map[string]int32{
		"CATALOG_UNSPECIFIED": 0,
		"CATALOG_STREAM":      1,
		"CATALOG_MEASURE":     2,
	}
)

func (x Catalog) Enum() *Catalog {
	p := new(Catalog)
	*p = x
	return p
}

func (x Catalog) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Catalog) Descriptor() protoreflect.EnumDescriptor {
	return file_banyandb_common_v2_common_proto_enumTypes[0].Descriptor()
}

func (Catalog) Type() protoreflect.EnumType {
	return &file_banyandb_common_v2_common_proto_enumTypes[0]
}

func (x Catalog) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Catalog.Descriptor instead.
func (Catalog) EnumDescriptor() ([]byte, []int) {
	return file_banyandb_common_v2_common_proto_rawDescGZIP(), []int{0}
}

// Metadata is for multi-tenant, multi-model use
type Metadata struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// group contains a set of options, like retention policy, max
	Group string `protobuf:"bytes,1,opt,name=group,proto3" json:"group,omitempty"`
	// name of the entity
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *Metadata) Reset() {
	*x = Metadata{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_common_v2_common_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Metadata) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Metadata) ProtoMessage() {}

func (x *Metadata) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_common_v2_common_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Metadata.ProtoReflect.Descriptor instead.
func (*Metadata) Descriptor() ([]byte, []int) {
	return file_banyandb_common_v2_common_proto_rawDescGZIP(), []int{0}
}

func (x *Metadata) GetGroup() string {
	if x != nil {
		return x.Group
	}
	return ""
}

func (x *Metadata) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

var File_banyandb_common_v2_common_proto protoreflect.FileDescriptor

var file_banyandb_common_v2_common_proto_rawDesc = []byte{
	0x0a, 0x1f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2f, 0x76, 0x32, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x12, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x22, 0x34, 0x0a, 0x08, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74,
	0x61, 0x12, 0x14, 0x0a, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x2a, 0x4b, 0x0a, 0x07, 0x43,
	0x61, 0x74, 0x61, 0x6c, 0x6f, 0x67, 0x12, 0x17, 0x0a, 0x13, 0x43, 0x41, 0x54, 0x41, 0x4c, 0x4f,
	0x47, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12,
	0x12, 0x0a, 0x0e, 0x43, 0x41, 0x54, 0x41, 0x4c, 0x4f, 0x47, 0x5f, 0x53, 0x54, 0x52, 0x45, 0x41,
	0x4d, 0x10, 0x01, 0x12, 0x13, 0x0a, 0x0f, 0x43, 0x41, 0x54, 0x41, 0x4c, 0x4f, 0x47, 0x5f, 0x4d,
	0x45, 0x41, 0x53, 0x55, 0x52, 0x45, 0x10, 0x02, 0x42, 0x6e, 0x0a, 0x28, 0x6f, 0x72, 0x67, 0x2e,
	0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e,
	0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2e, 0x76, 0x32, 0x5a, 0x42, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2f, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69,
	0x6e, 0x67, 0x2d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x32, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_common_v2_common_proto_rawDescOnce sync.Once
	file_banyandb_common_v2_common_proto_rawDescData = file_banyandb_common_v2_common_proto_rawDesc
)

func file_banyandb_common_v2_common_proto_rawDescGZIP() []byte {
	file_banyandb_common_v2_common_proto_rawDescOnce.Do(func() {
		file_banyandb_common_v2_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_common_v2_common_proto_rawDescData)
	})
	return file_banyandb_common_v2_common_proto_rawDescData
}

var file_banyandb_common_v2_common_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_banyandb_common_v2_common_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_banyandb_common_v2_common_proto_goTypes = []interface{}{
	(Catalog)(0),     // 0: banyandb.common.v2.Catalog
	(*Metadata)(nil), // 1: banyandb.common.v2.Metadata
}
var file_banyandb_common_v2_common_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_banyandb_common_v2_common_proto_init() }
func file_banyandb_common_v2_common_proto_init() {
	if File_banyandb_common_v2_common_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_banyandb_common_v2_common_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Metadata); i {
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
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_banyandb_common_v2_common_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_common_v2_common_proto_goTypes,
		DependencyIndexes: file_banyandb_common_v2_common_proto_depIdxs,
		EnumInfos:         file_banyandb_common_v2_common_proto_enumTypes,
		MessageInfos:      file_banyandb_common_v2_common_proto_msgTypes,
	}.Build()
	File_banyandb_common_v2_common_proto = out.File
	file_banyandb_common_v2_common_proto_rawDesc = nil
	file_banyandb_common_v2_common_proto_goTypes = nil
	file_banyandb_common_v2_common_proto_depIdxs = nil
}
