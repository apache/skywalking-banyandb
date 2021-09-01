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
// source: banyandb/stream/v2/write.proto

package v2

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	v21 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ElementValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// element_id could be span_id of a Span or segment_id of a Segment in the context of stream
	ElementId string `protobuf:"bytes,1,opt,name=element_id,json=elementId,proto3" json:"element_id,omitempty"`
	// timestamp_nanoseconds is in the timeunit of nanoseconds. It represents
	// 1) either the start time of a Span/Segment,
	// 2) or the timestamp of a log
	Timestamp *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// the order of tag_families' items match the stream schema
	TagFamilies []*ElementValue_TagFamily `protobuf:"bytes,3,rep,name=tag_families,json=tagFamilies,proto3" json:"tag_families,omitempty"`
}

func (x *ElementValue) Reset() {
	*x = ElementValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_stream_v2_write_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ElementValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ElementValue) ProtoMessage() {}

func (x *ElementValue) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_stream_v2_write_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ElementValue.ProtoReflect.Descriptor instead.
func (*ElementValue) Descriptor() ([]byte, []int) {
	return file_banyandb_stream_v2_write_proto_rawDescGZIP(), []int{0}
}

func (x *ElementValue) GetElementId() string {
	if x != nil {
		return x.ElementId
	}
	return ""
}

func (x *ElementValue) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *ElementValue) GetTagFamilies() []*ElementValue_TagFamily {
	if x != nil {
		return x.TagFamilies
	}
	return nil
}

type WriteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// the metadata is only required in the first write.
	Metadata *v2.Metadata `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// the element is required.
	Element *ElementValue `protobuf:"bytes,2,opt,name=element,proto3" json:"element,omitempty"`
}

func (x *WriteRequest) Reset() {
	*x = WriteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_stream_v2_write_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteRequest) ProtoMessage() {}

func (x *WriteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_stream_v2_write_proto_msgTypes[1]
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
	return file_banyandb_stream_v2_write_proto_rawDescGZIP(), []int{1}
}

func (x *WriteRequest) GetMetadata() *v2.Metadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *WriteRequest) GetElement() *ElementValue {
	if x != nil {
		return x.Element
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
		mi := &file_banyandb_stream_v2_write_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteResponse) ProtoMessage() {}

func (x *WriteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_stream_v2_write_proto_msgTypes[2]
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
	return file_banyandb_stream_v2_write_proto_rawDescGZIP(), []int{2}
}

type ElementValue_TagFamily struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tags []*v21.Tag `protobuf:"bytes,1,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *ElementValue_TagFamily) Reset() {
	*x = ElementValue_TagFamily{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_stream_v2_write_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ElementValue_TagFamily) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ElementValue_TagFamily) ProtoMessage() {}

func (x *ElementValue_TagFamily) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_stream_v2_write_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ElementValue_TagFamily.ProtoReflect.Descriptor instead.
func (*ElementValue_TagFamily) Descriptor() ([]byte, []int) {
	return file_banyandb_stream_v2_write_proto_rawDescGZIP(), []int{0, 0}
}

func (x *ElementValue_TagFamily) GetTags() []*v21.Tag {
	if x != nil {
		return x.Tags
	}
	return nil
}

var File_banyandb_stream_v2_write_proto protoreflect.FileDescriptor

var file_banyandb_stream_v2_write_proto_rawDesc = []byte{
	0x0a, 0x1e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x2f, 0x76, 0x32, 0x2f, 0x77, 0x72, 0x69, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x12, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x2e, 0x76, 0x32, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x32, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62,
	0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2f, 0x76, 0x32, 0x2f, 0x77, 0x72, 0x69, 0x74, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xef, 0x01, 0x0a, 0x0c, 0x45, 0x6c, 0x65, 0x6d, 0x65, 0x6e,
	0x74, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x6e,
	0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x65, 0x6c, 0x65, 0x6d,
	0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x38, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12,
	0x4d, 0x0a, 0x0c, 0x74, 0x61, 0x67, 0x5f, 0x66, 0x61, 0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73, 0x18,
	0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62,
	0x2e, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x76, 0x32, 0x2e, 0x45, 0x6c, 0x65, 0x6d, 0x65,
	0x6e, 0x74, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x54, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c,
	0x79, 0x52, 0x0b, 0x74, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73, 0x1a, 0x37,
	0x0a, 0x09, 0x54, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x12, 0x2a, 0x0a, 0x04, 0x74,
	0x61, 0x67, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79,
	0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x32, 0x2e, 0x54, 0x61,
	0x67, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x22, 0x84, 0x01, 0x0a, 0x0c, 0x57, 0x72, 0x69, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x38, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61,
	0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x32, 0x2e,
	0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x3a, 0x0a, 0x07, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x73,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x76, 0x32, 0x2e, 0x45, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74,
	0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x07, 0x65, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x22, 0x0f,
	0x0a, 0x0d, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42,
	0x6e, 0x0a, 0x28, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e, 0x73, 0x6b,
	0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64,
	0x62, 0x2e, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x76, 0x32, 0x5a, 0x42, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2f, 0x73,
	0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e,
	0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2f, 0x76, 0x32, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_stream_v2_write_proto_rawDescOnce sync.Once
	file_banyandb_stream_v2_write_proto_rawDescData = file_banyandb_stream_v2_write_proto_rawDesc
)

func file_banyandb_stream_v2_write_proto_rawDescGZIP() []byte {
	file_banyandb_stream_v2_write_proto_rawDescOnce.Do(func() {
		file_banyandb_stream_v2_write_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_stream_v2_write_proto_rawDescData)
	})
	return file_banyandb_stream_v2_write_proto_rawDescData
}

var file_banyandb_stream_v2_write_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_banyandb_stream_v2_write_proto_goTypes = []interface{}{
	(*ElementValue)(nil),           // 0: banyandb.stream.v2.ElementValue
	(*WriteRequest)(nil),           // 1: banyandb.stream.v2.WriteRequest
	(*WriteResponse)(nil),          // 2: banyandb.stream.v2.WriteResponse
	(*ElementValue_TagFamily)(nil), // 3: banyandb.stream.v2.ElementValue.TagFamily
	(*timestamppb.Timestamp)(nil),  // 4: google.protobuf.Timestamp
	(*v2.Metadata)(nil),            // 5: banyandb.common.v2.Metadata
	(*v21.Tag)(nil),                // 6: banyandb.model.v2.Tag
}
var file_banyandb_stream_v2_write_proto_depIdxs = []int32{
	4, // 0: banyandb.stream.v2.ElementValue.timestamp:type_name -> google.protobuf.Timestamp
	3, // 1: banyandb.stream.v2.ElementValue.tag_families:type_name -> banyandb.stream.v2.ElementValue.TagFamily
	5, // 2: banyandb.stream.v2.WriteRequest.metadata:type_name -> banyandb.common.v2.Metadata
	0, // 3: banyandb.stream.v2.WriteRequest.element:type_name -> banyandb.stream.v2.ElementValue
	6, // 4: banyandb.stream.v2.ElementValue.TagFamily.tags:type_name -> banyandb.model.v2.Tag
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_banyandb_stream_v2_write_proto_init() }
func file_banyandb_stream_v2_write_proto_init() {
	if File_banyandb_stream_v2_write_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_banyandb_stream_v2_write_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ElementValue); i {
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
		file_banyandb_stream_v2_write_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_stream_v2_write_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_stream_v2_write_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ElementValue_TagFamily); i {
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
			RawDescriptor: file_banyandb_stream_v2_write_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_stream_v2_write_proto_goTypes,
		DependencyIndexes: file_banyandb_stream_v2_write_proto_depIdxs,
		MessageInfos:      file_banyandb_stream_v2_write_proto_msgTypes,
	}.Build()
	File_banyandb_stream_v2_write_proto = out.File
	file_banyandb_stream_v2_write_proto_rawDesc = nil
	file_banyandb_stream_v2_write_proto_goTypes = nil
	file_banyandb_stream_v2_write_proto_depIdxs = nil
}
