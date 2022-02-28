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
// 	protoc        (unknown)
// source: banyandb/measure/v1/query.proto

package v1

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	v11 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// DataPoint is stored in Measures
type DataPoint struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// timestamp is in the timeunit of nanoseconds.
	Timestamp *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// tag_families contains tags selected in the projection
	TagFamilies []*v1.TagFamily `protobuf:"bytes,2,rep,name=tag_families,json=tagFamilies,proto3" json:"tag_families,omitempty"`
	// fields contains fields selected in the projection
	Fields []*DataPoint_Field `protobuf:"bytes,3,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (x *DataPoint) Reset() {
	*x = DataPoint{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DataPoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DataPoint) ProtoMessage() {}

func (x *DataPoint) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DataPoint.ProtoReflect.Descriptor instead.
func (*DataPoint) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{0}
}

func (x *DataPoint) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *DataPoint) GetTagFamilies() []*v1.TagFamily {
	if x != nil {
		return x.TagFamilies
	}
	return nil
}

func (x *DataPoint) GetFields() []*DataPoint_Field {
	if x != nil {
		return x.Fields
	}
	return nil
}

// QueryResponse is the response for a query to the Query module.
type QueryResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// data_points are the actual data returned
	DataPoints []*DataPoint `protobuf:"bytes,1,rep,name=data_points,json=dataPoints,proto3" json:"data_points,omitempty"`
}

func (x *QueryResponse) Reset() {
	*x = QueryResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryResponse) ProtoMessage() {}

func (x *QueryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryResponse.ProtoReflect.Descriptor instead.
func (*QueryResponse) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{1}
}

func (x *QueryResponse) GetDataPoints() []*DataPoint {
	if x != nil {
		return x.DataPoints
	}
	return nil
}

// QueryRequest is the request contract for query.
type QueryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// metadata is required
	Metadata *v11.Metadata `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// time_range is a range query with begin/end time of entities in the timeunit of nanoseconds.
	TimeRange *v1.TimeRange `protobuf:"bytes,2,opt,name=time_range,json=timeRange,proto3" json:"time_range,omitempty"`
	// criteria select the data points.
	Criteria []*QueryRequest_Criteria `protobuf:"bytes,4,rep,name=criteria,proto3" json:"criteria,omitempty"`
	// tag_projection can be used to select tags of the data points in the response
	TagProjection *v1.TagProjection `protobuf:"bytes,5,opt,name=tag_projection,json=tagProjection,proto3" json:"tag_projection,omitempty"`
	// field_projection can be used to select fields of the data points in the response
	FieldProjection *QueryRequest_FieldProjection `protobuf:"bytes,6,opt,name=field_projection,json=fieldProjection,proto3" json:"field_projection,omitempty"`
	// group_by groups data points based on their field value for sepcific tags and this field'name
	GroupBy *QueryRequest_GroupBy `protobuf:"bytes,7,opt,name=group_by,json=groupBy,proto3" json:"group_by,omitempty"`
	// agg aggregates data points based on a field
	Agg *QueryRequest_Aggregation `protobuf:"bytes,8,opt,name=agg,proto3" json:"agg,omitempty"`
}

func (x *QueryRequest) Reset() {
	*x = QueryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest) ProtoMessage() {}

func (x *QueryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest.ProtoReflect.Descriptor instead.
func (*QueryRequest) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{2}
}

func (x *QueryRequest) GetMetadata() *v11.Metadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *QueryRequest) GetTimeRange() *v1.TimeRange {
	if x != nil {
		return x.TimeRange
	}
	return nil
}

func (x *QueryRequest) GetCriteria() []*QueryRequest_Criteria {
	if x != nil {
		return x.Criteria
	}
	return nil
}

func (x *QueryRequest) GetTagProjection() *v1.TagProjection {
	if x != nil {
		return x.TagProjection
	}
	return nil
}

func (x *QueryRequest) GetFieldProjection() *QueryRequest_FieldProjection {
	if x != nil {
		return x.FieldProjection
	}
	return nil
}

func (x *QueryRequest) GetGroupBy() *QueryRequest_GroupBy {
	if x != nil {
		return x.GroupBy
	}
	return nil
}

func (x *QueryRequest) GetAgg() *QueryRequest_Aggregation {
	if x != nil {
		return x.Agg
	}
	return nil
}

type DataPoint_Field struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name  string         `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value *v1.FieldValue `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *DataPoint_Field) Reset() {
	*x = DataPoint_Field{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DataPoint_Field) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DataPoint_Field) ProtoMessage() {}

func (x *DataPoint_Field) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DataPoint_Field.ProtoReflect.Descriptor instead.
func (*DataPoint_Field) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{0, 0}
}

func (x *DataPoint_Field) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *DataPoint_Field) GetValue() *v1.FieldValue {
	if x != nil {
		return x.Value
	}
	return nil
}

type QueryRequest_Criteria struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TagFamilyName string          `protobuf:"bytes,1,opt,name=tag_family_name,json=tagFamilyName,proto3" json:"tag_family_name,omitempty"`
	Conditions    []*v1.Condition `protobuf:"bytes,2,rep,name=conditions,proto3" json:"conditions,omitempty"`
}

func (x *QueryRequest_Criteria) Reset() {
	*x = QueryRequest_Criteria{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest_Criteria) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest_Criteria) ProtoMessage() {}

func (x *QueryRequest_Criteria) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest_Criteria.ProtoReflect.Descriptor instead.
func (*QueryRequest_Criteria) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{2, 0}
}

func (x *QueryRequest_Criteria) GetTagFamilyName() string {
	if x != nil {
		return x.TagFamilyName
	}
	return ""
}

func (x *QueryRequest_Criteria) GetConditions() []*v1.Condition {
	if x != nil {
		return x.Conditions
	}
	return nil
}

type QueryRequest_FieldProjection struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Names []string `protobuf:"bytes,1,rep,name=names,proto3" json:"names,omitempty"`
}

func (x *QueryRequest_FieldProjection) Reset() {
	*x = QueryRequest_FieldProjection{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest_FieldProjection) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest_FieldProjection) ProtoMessage() {}

func (x *QueryRequest_FieldProjection) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest_FieldProjection.ProtoReflect.Descriptor instead.
func (*QueryRequest_FieldProjection) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{2, 1}
}

func (x *QueryRequest_FieldProjection) GetNames() []string {
	if x != nil {
		return x.Names
	}
	return nil
}

type QueryRequest_GroupBy struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TagProjection *v1.TagProjection `protobuf:"bytes,1,opt,name=tag_projection,json=tagProjection,proto3" json:"tag_projection,omitempty"`
	FieldName     string            `protobuf:"bytes,2,opt,name=field_name,json=fieldName,proto3" json:"field_name,omitempty"`
}

func (x *QueryRequest_GroupBy) Reset() {
	*x = QueryRequest_GroupBy{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest_GroupBy) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest_GroupBy) ProtoMessage() {}

func (x *QueryRequest_GroupBy) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest_GroupBy.ProtoReflect.Descriptor instead.
func (*QueryRequest_GroupBy) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{2, 2}
}

func (x *QueryRequest_GroupBy) GetTagProjection() *v1.TagProjection {
	if x != nil {
		return x.TagProjection
	}
	return nil
}

func (x *QueryRequest_GroupBy) GetFieldName() string {
	if x != nil {
		return x.FieldName
	}
	return ""
}

type QueryRequest_Aggregation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Function  v1.AggregationFunction `protobuf:"varint,1,opt,name=function,proto3,enum=banyandb.model.v1.AggregationFunction" json:"function,omitempty"`
	FieldName string                 `protobuf:"bytes,2,opt,name=field_name,json=fieldName,proto3" json:"field_name,omitempty"`
}

func (x *QueryRequest_Aggregation) Reset() {
	*x = QueryRequest_Aggregation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_measure_v1_query_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest_Aggregation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest_Aggregation) ProtoMessage() {}

func (x *QueryRequest_Aggregation) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_measure_v1_query_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest_Aggregation.ProtoReflect.Descriptor instead.
func (*QueryRequest_Aggregation) Descriptor() ([]byte, []int) {
	return file_banyandb_measure_v1_query_proto_rawDescGZIP(), []int{2, 3}
}

func (x *QueryRequest_Aggregation) GetFunction() v1.AggregationFunction {
	if x != nil {
		return x.Function
	}
	return v1.AggregationFunction(0)
}

func (x *QueryRequest_Aggregation) GetFieldName() string {
	if x != nil {
		return x.FieldName
	}
	return ""
}

var File_banyandb_measure_v1_query_proto protoreflect.FileDescriptor

var file_banyandb_measure_v1_query_proto_rawDesc = []byte{
	0x0a, 0x1f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x65, 0x61, 0x73, 0x75,
	0x72, 0x65, 0x2f, 0x76, 0x31, 0x2f, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x13, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73,
	0x75, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64,
	0x62, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e,
	0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e,
	0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2f, 0x76, 0x31, 0x2f, 0x71, 0x75, 0x65, 0x72,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x96, 0x02, 0x0a, 0x09, 0x44, 0x61, 0x74, 0x61,
	0x50, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x38, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12,
	0x3f, 0x0a, 0x0c, 0x74, 0x61, 0x67, 0x5f, 0x66, 0x61, 0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62,
	0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x67, 0x46, 0x61, 0x6d,
	0x69, 0x6c, 0x79, 0x52, 0x0b, 0x74, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73,
	0x12, 0x3c, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x24, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73,
	0x75, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x50, 0x6f, 0x69, 0x6e, 0x74,
	0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x1a, 0x50,
	0x0a, 0x05, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x33, 0x0a, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x46,
	0x69, 0x65, 0x6c, 0x64, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x22, 0x50, 0x0a, 0x0d, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x3f, 0x0a, 0x0b, 0x64, 0x61, 0x74, 0x61, 0x5f, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64,
	0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74,
	0x61, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x0a, 0x64, 0x61, 0x74, 0x61, 0x50, 0x6f, 0x69, 0x6e,
	0x74, 0x73, 0x22, 0xfb, 0x06, 0x0a, 0x0c, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x38, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x3b, 0x0a,
	0x0a, 0x74, 0x69, 0x6d, 0x65, 0x5f, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64,
	0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x46, 0x0a, 0x08, 0x63, 0x72,
	0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e,
	0x43, 0x72, 0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x52, 0x08, 0x63, 0x72, 0x69, 0x74, 0x65, 0x72,
	0x69, 0x61, 0x12, 0x47, 0x0a, 0x0e, 0x74, 0x61, 0x67, 0x5f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x54,
	0x61, 0x67, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0d, 0x74, 0x61,
	0x67, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x5c, 0x0a, 0x10, 0x66,
	0x69, 0x65, 0x6c, 0x64, 0x5f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x31, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62,
	0x2e, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72,
	0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x50, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0f, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x50,
	0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x44, 0x0a, 0x08, 0x67, 0x72, 0x6f,
	0x75, 0x70, 0x5f, 0x62, 0x79, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x47,
	0x72, 0x6f, 0x75, 0x70, 0x42, 0x79, 0x52, 0x07, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x42, 0x79, 0x12,
	0x3f, 0x0a, 0x03, 0x61, 0x67, 0x67, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e,
	0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x03, 0x61, 0x67, 0x67,
	0x1a, 0x70, 0x0a, 0x08, 0x43, 0x72, 0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x12, 0x26, 0x0a, 0x0f,
	0x74, 0x61, 0x67, 0x5f, 0x66, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x74, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79,
	0x4e, 0x61, 0x6d, 0x65, 0x12, 0x3c, 0x0a, 0x0a, 0x63, 0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6e,
	0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0a, 0x63, 0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x1a, 0x27, 0x0a, 0x0f, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x50, 0x72, 0x6f, 0x6a, 0x65,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x09, 0x52, 0x05, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x1a, 0x71, 0x0a, 0x07, 0x47,
	0x72, 0x6f, 0x75, 0x70, 0x42, 0x79, 0x12, 0x47, 0x0a, 0x0e, 0x74, 0x61, 0x67, 0x5f, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x20,
	0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x76, 0x31, 0x2e, 0x54, 0x61, 0x67, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x0d, 0x74, 0x61, 0x67, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12,
	0x1d, 0x0a, 0x0a, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x09, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x1a, 0x70,
	0x0a, 0x0b, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x42, 0x0a,
	0x08, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x26, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2e, 0x76, 0x31, 0x2e, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x46,
	0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x08, 0x66, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x4e, 0x61, 0x6d, 0x65,
	0x42, 0x70, 0x0a, 0x29, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e, 0x73,
	0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e,
	0x64, 0x62, 0x2e, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x5a, 0x43, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65,
	0x2f, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2d, 0x62, 0x61, 0x6e, 0x79,
	0x61, 0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x2f,
	0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_measure_v1_query_proto_rawDescOnce sync.Once
	file_banyandb_measure_v1_query_proto_rawDescData = file_banyandb_measure_v1_query_proto_rawDesc
)

func file_banyandb_measure_v1_query_proto_rawDescGZIP() []byte {
	file_banyandb_measure_v1_query_proto_rawDescOnce.Do(func() {
		file_banyandb_measure_v1_query_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_measure_v1_query_proto_rawDescData)
	})
	return file_banyandb_measure_v1_query_proto_rawDescData
}

var file_banyandb_measure_v1_query_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_banyandb_measure_v1_query_proto_goTypes = []interface{}{
	(*DataPoint)(nil),                    // 0: banyandb.measure.v1.DataPoint
	(*QueryResponse)(nil),                // 1: banyandb.measure.v1.QueryResponse
	(*QueryRequest)(nil),                 // 2: banyandb.measure.v1.QueryRequest
	(*DataPoint_Field)(nil),              // 3: banyandb.measure.v1.DataPoint.Field
	(*QueryRequest_Criteria)(nil),        // 4: banyandb.measure.v1.QueryRequest.Criteria
	(*QueryRequest_FieldProjection)(nil), // 5: banyandb.measure.v1.QueryRequest.FieldProjection
	(*QueryRequest_GroupBy)(nil),         // 6: banyandb.measure.v1.QueryRequest.GroupBy
	(*QueryRequest_Aggregation)(nil),     // 7: banyandb.measure.v1.QueryRequest.Aggregation
	(*timestamppb.Timestamp)(nil),        // 8: google.protobuf.Timestamp
	(*v1.TagFamily)(nil),                 // 9: banyandb.model.v1.TagFamily
	(*v11.Metadata)(nil),                 // 10: banyandb.common.v1.Metadata
	(*v1.TimeRange)(nil),                 // 11: banyandb.model.v1.TimeRange
	(*v1.TagProjection)(nil),             // 12: banyandb.model.v1.TagProjection
	(*v1.FieldValue)(nil),                // 13: banyandb.model.v1.FieldValue
	(*v1.Condition)(nil),                 // 14: banyandb.model.v1.Condition
	(v1.AggregationFunction)(0),          // 15: banyandb.model.v1.AggregationFunction
}
var file_banyandb_measure_v1_query_proto_depIdxs = []int32{
	8,  // 0: banyandb.measure.v1.DataPoint.timestamp:type_name -> google.protobuf.Timestamp
	9,  // 1: banyandb.measure.v1.DataPoint.tag_families:type_name -> banyandb.model.v1.TagFamily
	3,  // 2: banyandb.measure.v1.DataPoint.fields:type_name -> banyandb.measure.v1.DataPoint.Field
	0,  // 3: banyandb.measure.v1.QueryResponse.data_points:type_name -> banyandb.measure.v1.DataPoint
	10, // 4: banyandb.measure.v1.QueryRequest.metadata:type_name -> banyandb.common.v1.Metadata
	11, // 5: banyandb.measure.v1.QueryRequest.time_range:type_name -> banyandb.model.v1.TimeRange
	4,  // 6: banyandb.measure.v1.QueryRequest.criteria:type_name -> banyandb.measure.v1.QueryRequest.Criteria
	12, // 7: banyandb.measure.v1.QueryRequest.tag_projection:type_name -> banyandb.model.v1.TagProjection
	5,  // 8: banyandb.measure.v1.QueryRequest.field_projection:type_name -> banyandb.measure.v1.QueryRequest.FieldProjection
	6,  // 9: banyandb.measure.v1.QueryRequest.group_by:type_name -> banyandb.measure.v1.QueryRequest.GroupBy
	7,  // 10: banyandb.measure.v1.QueryRequest.agg:type_name -> banyandb.measure.v1.QueryRequest.Aggregation
	13, // 11: banyandb.measure.v1.DataPoint.Field.value:type_name -> banyandb.model.v1.FieldValue
	14, // 12: banyandb.measure.v1.QueryRequest.Criteria.conditions:type_name -> banyandb.model.v1.Condition
	12, // 13: banyandb.measure.v1.QueryRequest.GroupBy.tag_projection:type_name -> banyandb.model.v1.TagProjection
	15, // 14: banyandb.measure.v1.QueryRequest.Aggregation.function:type_name -> banyandb.model.v1.AggregationFunction
	15, // [15:15] is the sub-list for method output_type
	15, // [15:15] is the sub-list for method input_type
	15, // [15:15] is the sub-list for extension type_name
	15, // [15:15] is the sub-list for extension extendee
	0,  // [0:15] is the sub-list for field type_name
}

func init() { file_banyandb_measure_v1_query_proto_init() }
func file_banyandb_measure_v1_query_proto_init() {
	if File_banyandb_measure_v1_query_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_banyandb_measure_v1_query_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DataPoint); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryResponse); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DataPoint_Field); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest_Criteria); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest_FieldProjection); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest_GroupBy); i {
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
		file_banyandb_measure_v1_query_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest_Aggregation); i {
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
			RawDescriptor: file_banyandb_measure_v1_query_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_measure_v1_query_proto_goTypes,
		DependencyIndexes: file_banyandb_measure_v1_query_proto_depIdxs,
		MessageInfos:      file_banyandb_measure_v1_query_proto_msgTypes,
	}.Build()
	File_banyandb_measure_v1_query_proto = out.File
	file_banyandb_measure_v1_query_proto_rawDesc = nil
	file_banyandb_measure_v1_query_proto_goTypes = nil
	file_banyandb_measure_v1_query_proto_depIdxs = nil
}
