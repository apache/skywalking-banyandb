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
// 	protoc-gen-go v1.28.0
// 	protoc        (unknown)
// source: banyandb/model/v1/query.proto

package v1

import (
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Sort int32

const (
	Sort_SORT_UNSPECIFIED Sort = 0
	Sort_SORT_DESC        Sort = 1
	Sort_SORT_ASC         Sort = 2
)

// Enum value maps for Sort.
var (
	Sort_name = map[int32]string{
		0: "SORT_UNSPECIFIED",
		1: "SORT_DESC",
		2: "SORT_ASC",
	}
	Sort_value = map[string]int32{
		"SORT_UNSPECIFIED": 0,
		"SORT_DESC":        1,
		"SORT_ASC":         2,
	}
)

func (x Sort) Enum() *Sort {
	p := new(Sort)
	*p = x
	return p
}

func (x Sort) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Sort) Descriptor() protoreflect.EnumDescriptor {
	return file_banyandb_model_v1_query_proto_enumTypes[0].Descriptor()
}

func (Sort) Type() protoreflect.EnumType {
	return &file_banyandb_model_v1_query_proto_enumTypes[0]
}

func (x Sort) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Sort.Descriptor instead.
func (Sort) EnumDescriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{0}
}

// BinaryOp specifies the operation imposed to the given query condition
// For EQ, NE, LT, GT, LE and GE, only one operand should be given, i.e. one-to-one relationship.
// HAVING and NOT_HAVING allow multi-value to be the operand such as array/vector, i.e. one-to-many relationship.
// For example, "keyA" contains "valueA" **and** "valueB"
// MATCH performances a full-text search if the tag is analyzed.
// The string value applies to the same analyzer as the tag, but string array value does not.
// Each item in a string array is seen as a token instead of a query expression.
type Condition_BinaryOp int32

const (
	Condition_BINARY_OP_UNSPECIFIED Condition_BinaryOp = 0
	Condition_BINARY_OP_EQ          Condition_BinaryOp = 1
	Condition_BINARY_OP_NE          Condition_BinaryOp = 2
	Condition_BINARY_OP_LT          Condition_BinaryOp = 3
	Condition_BINARY_OP_GT          Condition_BinaryOp = 4
	Condition_BINARY_OP_LE          Condition_BinaryOp = 5
	Condition_BINARY_OP_GE          Condition_BinaryOp = 6
	Condition_BINARY_OP_HAVING      Condition_BinaryOp = 7
	Condition_BINARY_OP_NOT_HAVING  Condition_BinaryOp = 8
	Condition_BINARY_OP_IN          Condition_BinaryOp = 9
	Condition_BINARY_OP_NOT_IN      Condition_BinaryOp = 10
	Condition_BINARY_OP_MATCH       Condition_BinaryOp = 11
)

// Enum value maps for Condition_BinaryOp.
var (
	Condition_BinaryOp_name = map[int32]string{
		0:  "BINARY_OP_UNSPECIFIED",
		1:  "BINARY_OP_EQ",
		2:  "BINARY_OP_NE",
		3:  "BINARY_OP_LT",
		4:  "BINARY_OP_GT",
		5:  "BINARY_OP_LE",
		6:  "BINARY_OP_GE",
		7:  "BINARY_OP_HAVING",
		8:  "BINARY_OP_NOT_HAVING",
		9:  "BINARY_OP_IN",
		10: "BINARY_OP_NOT_IN",
		11: "BINARY_OP_MATCH",
	}
	Condition_BinaryOp_value = map[string]int32{
		"BINARY_OP_UNSPECIFIED": 0,
		"BINARY_OP_EQ":          1,
		"BINARY_OP_NE":          2,
		"BINARY_OP_LT":          3,
		"BINARY_OP_GT":          4,
		"BINARY_OP_LE":          5,
		"BINARY_OP_GE":          6,
		"BINARY_OP_HAVING":      7,
		"BINARY_OP_NOT_HAVING":  8,
		"BINARY_OP_IN":          9,
		"BINARY_OP_NOT_IN":      10,
		"BINARY_OP_MATCH":       11,
	}
)

func (x Condition_BinaryOp) Enum() *Condition_BinaryOp {
	p := new(Condition_BinaryOp)
	*p = x
	return p
}

func (x Condition_BinaryOp) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Condition_BinaryOp) Descriptor() protoreflect.EnumDescriptor {
	return file_banyandb_model_v1_query_proto_enumTypes[1].Descriptor()
}

func (Condition_BinaryOp) Type() protoreflect.EnumType {
	return &file_banyandb_model_v1_query_proto_enumTypes[1]
}

func (x Condition_BinaryOp) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Condition_BinaryOp.Descriptor instead.
func (Condition_BinaryOp) EnumDescriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{2, 0}
}

type LogicalExpression_LogicalOp int32

const (
	LogicalExpression_LOGICAL_OP_UNSPECIFIED LogicalExpression_LogicalOp = 0
	LogicalExpression_LOGICAL_OP_AND         LogicalExpression_LogicalOp = 1
	LogicalExpression_LOGICAL_OP_OR          LogicalExpression_LogicalOp = 2
)

// Enum value maps for LogicalExpression_LogicalOp.
var (
	LogicalExpression_LogicalOp_name = map[int32]string{
		0: "LOGICAL_OP_UNSPECIFIED",
		1: "LOGICAL_OP_AND",
		2: "LOGICAL_OP_OR",
	}
	LogicalExpression_LogicalOp_value = map[string]int32{
		"LOGICAL_OP_UNSPECIFIED": 0,
		"LOGICAL_OP_AND":         1,
		"LOGICAL_OP_OR":          2,
	}
)

func (x LogicalExpression_LogicalOp) Enum() *LogicalExpression_LogicalOp {
	p := new(LogicalExpression_LogicalOp)
	*p = x
	return p
}

func (x LogicalExpression_LogicalOp) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (LogicalExpression_LogicalOp) Descriptor() protoreflect.EnumDescriptor {
	return file_banyandb_model_v1_query_proto_enumTypes[2].Descriptor()
}

func (LogicalExpression_LogicalOp) Type() protoreflect.EnumType {
	return &file_banyandb_model_v1_query_proto_enumTypes[2]
}

func (x LogicalExpression_LogicalOp) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use LogicalExpression_LogicalOp.Descriptor instead.
func (LogicalExpression_LogicalOp) EnumDescriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{4, 0}
}

// Pair is the building block of a record which is equivalent to a key-value pair.
// In the context of Trace, it could be metadata of a trace such as service_name, service_instance, etc.
// Besides, other tags are organized in key-value pair in the underlying storage layer.
// One should notice that the values can be a multi-value.
type Tag struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string    `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value *TagValue `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Tag) Reset() {
	*x = Tag{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Tag) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tag) ProtoMessage() {}

func (x *Tag) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tag.ProtoReflect.Descriptor instead.
func (*Tag) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{0}
}

func (x *Tag) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Tag) GetValue() *TagValue {
	if x != nil {
		return x.Value
	}
	return nil
}

type TagFamily struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Tags []*Tag `protobuf:"bytes,2,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *TagFamily) Reset() {
	*x = TagFamily{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TagFamily) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagFamily) ProtoMessage() {}

func (x *TagFamily) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagFamily.ProtoReflect.Descriptor instead.
func (*TagFamily) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{1}
}

func (x *TagFamily) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TagFamily) GetTags() []*Tag {
	if x != nil {
		return x.Tags
	}
	return nil
}

// Condition consists of the query condition with a single binary operator to be imposed
// For 1:1 BinaryOp, values in condition must be an array with length = 1,
// while for 1:N BinaryOp, values can be an array with length >= 1.
type Condition struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name  string             `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Op    Condition_BinaryOp `protobuf:"varint,2,opt,name=op,proto3,enum=banyandb.model.v1.Condition_BinaryOp" json:"op,omitempty"`
	Value *TagValue          `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Condition) Reset() {
	*x = Condition{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Condition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Condition) ProtoMessage() {}

func (x *Condition) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Condition.ProtoReflect.Descriptor instead.
func (*Condition) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{2}
}

func (x *Condition) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Condition) GetOp() Condition_BinaryOp {
	if x != nil {
		return x.Op
	}
	return Condition_BINARY_OP_UNSPECIFIED
}

func (x *Condition) GetValue() *TagValue {
	if x != nil {
		return x.Value
	}
	return nil
}

// tag_families are indexed.
type Criteria struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Exp:
	//
	//	*Criteria_Le
	//	*Criteria_Condition
	Exp isCriteria_Exp `protobuf_oneof:"exp"`
}

func (x *Criteria) Reset() {
	*x = Criteria{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Criteria) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Criteria) ProtoMessage() {}

func (x *Criteria) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Criteria.ProtoReflect.Descriptor instead.
func (*Criteria) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{3}
}

func (m *Criteria) GetExp() isCriteria_Exp {
	if m != nil {
		return m.Exp
	}
	return nil
}

func (x *Criteria) GetLe() *LogicalExpression {
	if x, ok := x.GetExp().(*Criteria_Le); ok {
		return x.Le
	}
	return nil
}

func (x *Criteria) GetCondition() *Condition {
	if x, ok := x.GetExp().(*Criteria_Condition); ok {
		return x.Condition
	}
	return nil
}

type isCriteria_Exp interface {
	isCriteria_Exp()
}

type Criteria_Le struct {
	Le *LogicalExpression `protobuf:"bytes,1,opt,name=le,proto3,oneof"`
}

type Criteria_Condition struct {
	Condition *Condition `protobuf:"bytes,2,opt,name=condition,proto3,oneof"`
}

func (*Criteria_Le) isCriteria_Exp() {}

func (*Criteria_Condition) isCriteria_Exp() {}

// LogicalExpression supports logical operation
type LogicalExpression struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// op is a logial operation
	Op    LogicalExpression_LogicalOp `protobuf:"varint,1,opt,name=op,proto3,enum=banyandb.model.v1.LogicalExpression_LogicalOp" json:"op,omitempty"`
	Left  *Criteria                   `protobuf:"bytes,2,opt,name=left,proto3" json:"left,omitempty"`
	Right *Criteria                   `protobuf:"bytes,3,opt,name=right,proto3" json:"right,omitempty"`
}

func (x *LogicalExpression) Reset() {
	*x = LogicalExpression{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LogicalExpression) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogicalExpression) ProtoMessage() {}

func (x *LogicalExpression) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogicalExpression.ProtoReflect.Descriptor instead.
func (*LogicalExpression) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{4}
}

func (x *LogicalExpression) GetOp() LogicalExpression_LogicalOp {
	if x != nil {
		return x.Op
	}
	return LogicalExpression_LOGICAL_OP_UNSPECIFIED
}

func (x *LogicalExpression) GetLeft() *Criteria {
	if x != nil {
		return x.Left
	}
	return nil
}

func (x *LogicalExpression) GetRight() *Criteria {
	if x != nil {
		return x.Right
	}
	return nil
}

// QueryOrder means a Sort operation to be done for a given index rule.
// The index_rule_name refers to the name of a index rule bound to the subject.
type QueryOrder struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IndexRuleName string `protobuf:"bytes,1,opt,name=index_rule_name,json=indexRuleName,proto3" json:"index_rule_name,omitempty"`
	Sort          Sort   `protobuf:"varint,2,opt,name=sort,proto3,enum=banyandb.model.v1.Sort" json:"sort,omitempty"`
}

func (x *QueryOrder) Reset() {
	*x = QueryOrder{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryOrder) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryOrder) ProtoMessage() {}

func (x *QueryOrder) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryOrder.ProtoReflect.Descriptor instead.
func (*QueryOrder) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{5}
}

func (x *QueryOrder) GetIndexRuleName() string {
	if x != nil {
		return x.IndexRuleName
	}
	return ""
}

func (x *QueryOrder) GetSort() Sort {
	if x != nil {
		return x.Sort
	}
	return Sort_SORT_UNSPECIFIED
}

// TagProjection is used to select the names of keys to be returned.
type TagProjection struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TagFamilies []*TagProjection_TagFamily `protobuf:"bytes,1,rep,name=tag_families,json=tagFamilies,proto3" json:"tag_families,omitempty"`
}

func (x *TagProjection) Reset() {
	*x = TagProjection{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TagProjection) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagProjection) ProtoMessage() {}

func (x *TagProjection) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagProjection.ProtoReflect.Descriptor instead.
func (*TagProjection) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{6}
}

func (x *TagProjection) GetTagFamilies() []*TagProjection_TagFamily {
	if x != nil {
		return x.TagFamilies
	}
	return nil
}

// TimeRange is a range query for uint64,
// the range here follows left-inclusive and right-exclusive rule, i.e. [begin, end) if both edges exist
type TimeRange struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Begin *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=begin,proto3" json:"begin,omitempty"`
	End   *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=end,proto3" json:"end,omitempty"`
}

func (x *TimeRange) Reset() {
	*x = TimeRange{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TimeRange) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TimeRange) ProtoMessage() {}

func (x *TimeRange) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TimeRange.ProtoReflect.Descriptor instead.
func (*TimeRange) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{7}
}

func (x *TimeRange) GetBegin() *timestamppb.Timestamp {
	if x != nil {
		return x.Begin
	}
	return nil
}

func (x *TimeRange) GetEnd() *timestamppb.Timestamp {
	if x != nil {
		return x.End
	}
	return nil
}

type TagProjection_TagFamily struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Tags []string `protobuf:"bytes,2,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *TagProjection_TagFamily) Reset() {
	*x = TagProjection_TagFamily{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_model_v1_query_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TagProjection_TagFamily) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagProjection_TagFamily) ProtoMessage() {}

func (x *TagProjection_TagFamily) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_model_v1_query_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagProjection_TagFamily.ProtoReflect.Descriptor instead.
func (*TagProjection_TagFamily) Descriptor() ([]byte, []int) {
	return file_banyandb_model_v1_query_proto_rawDescGZIP(), []int{6, 0}
}

func (x *TagProjection_TagFamily) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TagProjection_TagFamily) GetTags() []string {
	if x != nil {
		return x.Tags
	}
	return nil
}

var File_banyandb_model_v1_query_proto protoreflect.FileDescriptor

var file_banyandb_model_v1_query_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2f, 0x76, 0x31, 0x2f, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x11, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x76, 0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f,
	0x64, 0x65, 0x6c, 0x2f, 0x76, 0x31, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x1a, 0x17, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61,
	0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x4a, 0x0a, 0x03,
	0x54, 0x61, 0x67, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x31, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e,
	0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x67, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x4b, 0x0a, 0x09, 0x54, 0x61, 0x67, 0x46,
	0x61, 0x6d, 0x69, 0x6c, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x2a, 0x0a, 0x04, 0x74, 0x61, 0x67,
	0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e,
	0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x67, 0x52,
	0x04, 0x74, 0x61, 0x67, 0x73, 0x22, 0x8a, 0x03, 0x0a, 0x09, 0x43, 0x6f, 0x6e, 0x64, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x35, 0x0a, 0x02, 0x6f, 0x70, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0e, 0x32, 0x25, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d,
	0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x4f, 0x70, 0x52, 0x02, 0x6f, 0x70, 0x12, 0x31,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e,
	0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76,
	0x31, 0x2e, 0x54, 0x61, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x22, 0xfe, 0x01, 0x0a, 0x08, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x4f, 0x70, 0x12, 0x19,
	0x0a, 0x15, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x55, 0x4e, 0x53, 0x50,
	0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x10, 0x0a, 0x0c, 0x42, 0x49, 0x4e,
	0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x45, 0x51, 0x10, 0x01, 0x12, 0x10, 0x0a, 0x0c, 0x42,
	0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x4e, 0x45, 0x10, 0x02, 0x12, 0x10, 0x0a,
	0x0c, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x4c, 0x54, 0x10, 0x03, 0x12,
	0x10, 0x0a, 0x0c, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x47, 0x54, 0x10,
	0x04, 0x12, 0x10, 0x0a, 0x0c, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x4c,
	0x45, 0x10, 0x05, 0x12, 0x10, 0x0a, 0x0c, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50,
	0x5f, 0x47, 0x45, 0x10, 0x06, 0x12, 0x14, 0x0a, 0x10, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f,
	0x4f, 0x50, 0x5f, 0x48, 0x41, 0x56, 0x49, 0x4e, 0x47, 0x10, 0x07, 0x12, 0x18, 0x0a, 0x14, 0x42,
	0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x4e, 0x4f, 0x54, 0x5f, 0x48, 0x41, 0x56,
	0x49, 0x4e, 0x47, 0x10, 0x08, 0x12, 0x10, 0x0a, 0x0c, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f,
	0x4f, 0x50, 0x5f, 0x49, 0x4e, 0x10, 0x09, 0x12, 0x14, 0x0a, 0x10, 0x42, 0x49, 0x4e, 0x41, 0x52,
	0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x4e, 0x4f, 0x54, 0x5f, 0x49, 0x4e, 0x10, 0x0a, 0x12, 0x13, 0x0a,
	0x0f, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4f, 0x50, 0x5f, 0x4d, 0x41, 0x54, 0x43, 0x48,
	0x10, 0x0b, 0x22, 0x87, 0x01, 0x0a, 0x08, 0x43, 0x72, 0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x12,
	0x36, 0x0a, 0x02, 0x6c, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x24, 0x2e, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e,
	0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x45, 0x78, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69, 0x6f,
	0x6e, 0x48, 0x00, 0x52, 0x02, 0x6c, 0x65, 0x12, 0x3c, 0x0a, 0x09, 0x63, 0x6f, 0x6e, 0x64, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x43,
	0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x48, 0x00, 0x52, 0x09, 0x63, 0x6f, 0x6e, 0x64,
	0x69, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x05, 0x0a, 0x03, 0x65, 0x78, 0x70, 0x22, 0x87, 0x02, 0x0a,
	0x11, 0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x45, 0x78, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x12, 0x3e, 0x0a, 0x02, 0x6f, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x2e,
	0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x76, 0x31, 0x2e, 0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x45, 0x78, 0x70, 0x72, 0x65, 0x73,
	0x73, 0x69, 0x6f, 0x6e, 0x2e, 0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x4f, 0x70, 0x52, 0x02,
	0x6f, 0x70, 0x12, 0x2f, 0x0a, 0x04, 0x6c, 0x65, 0x66, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1b, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65,
	0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x72, 0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x52, 0x04, 0x6c,
	0x65, 0x66, 0x74, 0x12, 0x31, 0x0a, 0x05, 0x72, 0x69, 0x67, 0x68, 0x74, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f,
	0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x72, 0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x52,
	0x05, 0x72, 0x69, 0x67, 0x68, 0x74, 0x22, 0x4e, 0x0a, 0x09, 0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61,
	0x6c, 0x4f, 0x70, 0x12, 0x1a, 0x0a, 0x16, 0x4c, 0x4f, 0x47, 0x49, 0x43, 0x41, 0x4c, 0x5f, 0x4f,
	0x50, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12,
	0x12, 0x0a, 0x0e, 0x4c, 0x4f, 0x47, 0x49, 0x43, 0x41, 0x4c, 0x5f, 0x4f, 0x50, 0x5f, 0x41, 0x4e,
	0x44, 0x10, 0x01, 0x12, 0x11, 0x0a, 0x0d, 0x4c, 0x4f, 0x47, 0x49, 0x43, 0x41, 0x4c, 0x5f, 0x4f,
	0x50, 0x5f, 0x4f, 0x52, 0x10, 0x02, 0x22, 0x61, 0x0a, 0x0a, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4f,
	0x72, 0x64, 0x65, 0x72, 0x12, 0x26, 0x0a, 0x0f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x5f, 0x72, 0x75,
	0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x69,
	0x6e, 0x64, 0x65, 0x78, 0x52, 0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2b, 0x0a, 0x04,
	0x73, 0x6f, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x17, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x53,
	0x6f, 0x72, 0x74, 0x52, 0x04, 0x73, 0x6f, 0x72, 0x74, 0x22, 0x9d, 0x01, 0x0a, 0x0d, 0x54, 0x61,
	0x67, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x57, 0x0a, 0x0c, 0x74,
	0x61, 0x67, 0x5f, 0x66, 0x61, 0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x2a, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64,
	0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x67, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x54, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x42, 0x08, 0xfa,
	0x42, 0x05, 0x92, 0x01, 0x02, 0x08, 0x01, 0x52, 0x0b, 0x74, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69,
	0x6c, 0x69, 0x65, 0x73, 0x1a, 0x33, 0x0a, 0x09, 0x54, 0x61, 0x67, 0x46, 0x61, 0x6d, 0x69, 0x6c,
	0x79, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x61, 0x67, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x09, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x22, 0x6b, 0x0a, 0x09, 0x54, 0x69, 0x6d,
	0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x30, 0x0a, 0x05, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x05, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x12, 0x2c, 0x0a, 0x03, 0x65, 0x6e, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x2a, 0x39, 0x0a, 0x04, 0x53, 0x6f, 0x72, 0x74, 0x12, 0x14,
	0x0a, 0x10, 0x53, 0x4f, 0x52, 0x54, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49,
	0x45, 0x44, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x53, 0x4f, 0x52, 0x54, 0x5f, 0x44, 0x45, 0x53,
	0x43, 0x10, 0x01, 0x12, 0x0c, 0x0a, 0x08, 0x53, 0x4f, 0x52, 0x54, 0x5f, 0x41, 0x53, 0x43, 0x10,
	0x02, 0x42, 0x6c, 0x0a, 0x27, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e,
	0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2e, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x5a, 0x41, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2f,
	0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2d, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2f, 0x76, 0x31, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_model_v1_query_proto_rawDescOnce sync.Once
	file_banyandb_model_v1_query_proto_rawDescData = file_banyandb_model_v1_query_proto_rawDesc
)

func file_banyandb_model_v1_query_proto_rawDescGZIP() []byte {
	file_banyandb_model_v1_query_proto_rawDescOnce.Do(func() {
		file_banyandb_model_v1_query_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_model_v1_query_proto_rawDescData)
	})
	return file_banyandb_model_v1_query_proto_rawDescData
}

var file_banyandb_model_v1_query_proto_enumTypes = make([]protoimpl.EnumInfo, 3)
var file_banyandb_model_v1_query_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_banyandb_model_v1_query_proto_goTypes = []interface{}{
	(Sort)(0),                        // 0: banyandb.model.v1.Sort
	(Condition_BinaryOp)(0),          // 1: banyandb.model.v1.Condition.BinaryOp
	(LogicalExpression_LogicalOp)(0), // 2: banyandb.model.v1.LogicalExpression.LogicalOp
	(*Tag)(nil),                      // 3: banyandb.model.v1.Tag
	(*TagFamily)(nil),                // 4: banyandb.model.v1.TagFamily
	(*Condition)(nil),                // 5: banyandb.model.v1.Condition
	(*Criteria)(nil),                 // 6: banyandb.model.v1.Criteria
	(*LogicalExpression)(nil),        // 7: banyandb.model.v1.LogicalExpression
	(*QueryOrder)(nil),               // 8: banyandb.model.v1.QueryOrder
	(*TagProjection)(nil),            // 9: banyandb.model.v1.TagProjection
	(*TimeRange)(nil),                // 10: banyandb.model.v1.TimeRange
	(*TagProjection_TagFamily)(nil),  // 11: banyandb.model.v1.TagProjection.TagFamily
	(*TagValue)(nil),                 // 12: banyandb.model.v1.TagValue
	(*timestamppb.Timestamp)(nil),    // 13: google.protobuf.Timestamp
}
var file_banyandb_model_v1_query_proto_depIdxs = []int32{
	12, // 0: banyandb.model.v1.Tag.value:type_name -> banyandb.model.v1.TagValue
	3,  // 1: banyandb.model.v1.TagFamily.tags:type_name -> banyandb.model.v1.Tag
	1,  // 2: banyandb.model.v1.Condition.op:type_name -> banyandb.model.v1.Condition.BinaryOp
	12, // 3: banyandb.model.v1.Condition.value:type_name -> banyandb.model.v1.TagValue
	7,  // 4: banyandb.model.v1.Criteria.le:type_name -> banyandb.model.v1.LogicalExpression
	5,  // 5: banyandb.model.v1.Criteria.condition:type_name -> banyandb.model.v1.Condition
	2,  // 6: banyandb.model.v1.LogicalExpression.op:type_name -> banyandb.model.v1.LogicalExpression.LogicalOp
	6,  // 7: banyandb.model.v1.LogicalExpression.left:type_name -> banyandb.model.v1.Criteria
	6,  // 8: banyandb.model.v1.LogicalExpression.right:type_name -> banyandb.model.v1.Criteria
	0,  // 9: banyandb.model.v1.QueryOrder.sort:type_name -> banyandb.model.v1.Sort
	11, // 10: banyandb.model.v1.TagProjection.tag_families:type_name -> banyandb.model.v1.TagProjection.TagFamily
	13, // 11: banyandb.model.v1.TimeRange.begin:type_name -> google.protobuf.Timestamp
	13, // 12: banyandb.model.v1.TimeRange.end:type_name -> google.protobuf.Timestamp
	13, // [13:13] is the sub-list for method output_type
	13, // [13:13] is the sub-list for method input_type
	13, // [13:13] is the sub-list for extension type_name
	13, // [13:13] is the sub-list for extension extendee
	0,  // [0:13] is the sub-list for field type_name
}

func init() { file_banyandb_model_v1_query_proto_init() }
func file_banyandb_model_v1_query_proto_init() {
	if File_banyandb_model_v1_query_proto != nil {
		return
	}
	file_banyandb_model_v1_common_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_banyandb_model_v1_query_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Tag); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TagFamily); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Condition); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Criteria); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LogicalExpression); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryOrder); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TagProjection); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TimeRange); i {
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
		file_banyandb_model_v1_query_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TagProjection_TagFamily); i {
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
	file_banyandb_model_v1_query_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*Criteria_Le)(nil),
		(*Criteria_Condition)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_banyandb_model_v1_query_proto_rawDesc,
			NumEnums:      3,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_model_v1_query_proto_goTypes,
		DependencyIndexes: file_banyandb_model_v1_query_proto_depIdxs,
		EnumInfos:         file_banyandb_model_v1_query_proto_enumTypes,
		MessageInfos:      file_banyandb_model_v1_query_proto_msgTypes,
	}.Build()
	File_banyandb_model_v1_query_proto = out.File
	file_banyandb_model_v1_query_proto_rawDesc = nil
	file_banyandb_model_v1_query_proto_goTypes = nil
	file_banyandb_model_v1_query_proto_depIdxs = nil
}
