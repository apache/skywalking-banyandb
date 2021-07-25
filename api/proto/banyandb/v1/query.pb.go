// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.17.3
// source: banyandb/v1/query.proto

package v1

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// BinaryOp specifies the operation imposed to the given query condition
// For EQ, NE, LT, GT, LE and GE, only one operand should be given, i.e. one-to-one relationship.
// HAVING and NOT_HAVING allow multi-value to be the operand such as array/vector, i.e. one-to-many relationship.
// For example, "keyA" contains "valueA" **and** "valueB"
type PairQuery_BinaryOp int32

const (
	PairQuery_EQ         PairQuery_BinaryOp = 0
	PairQuery_NE         PairQuery_BinaryOp = 1
	PairQuery_LT         PairQuery_BinaryOp = 2
	PairQuery_GT         PairQuery_BinaryOp = 3
	PairQuery_LE         PairQuery_BinaryOp = 4
	PairQuery_GE         PairQuery_BinaryOp = 5
	PairQuery_HAVING     PairQuery_BinaryOp = 6
	PairQuery_NOT_HAVING PairQuery_BinaryOp = 7
)

// Enum value maps for PairQuery_BinaryOp.
var (
	PairQuery_BinaryOp_name = map[int32]string{
		0: "EQ",
		1: "NE",
		2: "LT",
		3: "GT",
		4: "LE",
		5: "GE",
		6: "HAVING",
		7: "NOT_HAVING",
	}
	PairQuery_BinaryOp_value = map[string]int32{
		"EQ":         0,
		"NE":         1,
		"LT":         2,
		"GT":         3,
		"LE":         4,
		"GE":         5,
		"HAVING":     6,
		"NOT_HAVING": 7,
	}
)

func (x PairQuery_BinaryOp) Enum() *PairQuery_BinaryOp {
	p := new(PairQuery_BinaryOp)
	*p = x
	return p
}

func (x PairQuery_BinaryOp) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (PairQuery_BinaryOp) Descriptor() protoreflect.EnumDescriptor {
	return file_banyandb_v1_query_proto_enumTypes[0].Descriptor()
}

func (PairQuery_BinaryOp) Type() protoreflect.EnumType {
	return &file_banyandb_v1_query_proto_enumTypes[0]
}

func (x PairQuery_BinaryOp) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use PairQuery_BinaryOp.Descriptor instead.
func (PairQuery_BinaryOp) EnumDescriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{3, 0}
}

type QueryOrder_Sort int32

const (
	QueryOrder_DESC QueryOrder_Sort = 0
	QueryOrder_ASC  QueryOrder_Sort = 1
)

// Enum value maps for QueryOrder_Sort.
var (
	QueryOrder_Sort_name = map[int32]string{
		0: "DESC",
		1: "ASC",
	}
	QueryOrder_Sort_value = map[string]int32{
		"DESC": 0,
		"ASC":  1,
	}
)

func (x QueryOrder_Sort) Enum() *QueryOrder_Sort {
	p := new(QueryOrder_Sort)
	*p = x
	return p
}

func (x QueryOrder_Sort) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (QueryOrder_Sort) Descriptor() protoreflect.EnumDescriptor {
	return file_banyandb_v1_query_proto_enumTypes[1].Descriptor()
}

func (QueryOrder_Sort) Type() protoreflect.EnumType {
	return &file_banyandb_v1_query_proto_enumTypes[1]
}

func (x QueryOrder_Sort) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use QueryOrder_Sort.Descriptor instead.
func (QueryOrder_Sort) EnumDescriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{4, 0}
}

// IntPair in a typed pair with an array of int64 as values
type IntPair struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key    string  `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Values []int64 `protobuf:"varint,2,rep,packed,name=values,proto3" json:"values,omitempty"`
}

func (x *IntPair) Reset() {
	*x = IntPair{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IntPair) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IntPair) ProtoMessage() {}

func (x *IntPair) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IntPair.ProtoReflect.Descriptor instead.
func (*IntPair) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{0}
}

func (x *IntPair) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *IntPair) GetValues() []int64 {
	if x != nil {
		return x.Values
	}
	return nil
}

// StrPair in a typed pair with an array of string as values
type StrPair struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key    string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Values []string `protobuf:"bytes,2,rep,name=values,proto3" json:"values,omitempty"`
}

func (x *StrPair) Reset() {
	*x = StrPair{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StrPair) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StrPair) ProtoMessage() {}

func (x *StrPair) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StrPair.ProtoReflect.Descriptor instead.
func (*StrPair) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{1}
}

func (x *StrPair) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *StrPair) GetValues() []string {
	if x != nil {
		return x.Values
	}
	return nil
}

// Pair is the building block of a record which is equivalent to a key-value pair.
// In the context of Trace, it could be metadata of a trace such as service_name, service_instance, etc.
// Besides, other fields/tags are organized in key-value pair in the underlying storage layer.
// One should notice that the values can be a multi-value.
type TypedPair struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Typed:
	//	*TypedPair_IntPair
	//	*TypedPair_StrPair
	Typed isTypedPair_Typed `protobuf_oneof:"typed"`
}

func (x *TypedPair) Reset() {
	*x = TypedPair{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TypedPair) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TypedPair) ProtoMessage() {}

func (x *TypedPair) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TypedPair.ProtoReflect.Descriptor instead.
func (*TypedPair) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{2}
}

func (m *TypedPair) GetTyped() isTypedPair_Typed {
	if m != nil {
		return m.Typed
	}
	return nil
}

func (x *TypedPair) GetIntPair() *IntPair {
	if x, ok := x.GetTyped().(*TypedPair_IntPair); ok {
		return x.IntPair
	}
	return nil
}

func (x *TypedPair) GetStrPair() *StrPair {
	if x, ok := x.GetTyped().(*TypedPair_StrPair); ok {
		return x.StrPair
	}
	return nil
}

type isTypedPair_Typed interface {
	isTypedPair_Typed()
}

type TypedPair_IntPair struct {
	IntPair *IntPair `protobuf:"bytes,1,opt,name=intPair,proto3,oneof"`
}

type TypedPair_StrPair struct {
	StrPair *StrPair `protobuf:"bytes,2,opt,name=strPair,proto3,oneof"`
}

func (*TypedPair_IntPair) isTypedPair_Typed() {}

func (*TypedPair_StrPair) isTypedPair_Typed() {}

// PairQuery consists of the query condition with a single binary operator to be imposed
// For 1:1 BinaryOp, values in condition must be an array with length = 1,
// while for 1:N BinaryOp, values can be an array with length >= 1.
type PairQuery struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Op        PairQuery_BinaryOp `protobuf:"varint,1,opt,name=op,proto3,enum=banyandb.v1.PairQuery_BinaryOp" json:"op,omitempty"`
	Condition *TypedPair         `protobuf:"bytes,2,opt,name=condition,proto3" json:"condition,omitempty"`
}

func (x *PairQuery) Reset() {
	*x = PairQuery{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PairQuery) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PairQuery) ProtoMessage() {}

func (x *PairQuery) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PairQuery.ProtoReflect.Descriptor instead.
func (*PairQuery) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{3}
}

func (x *PairQuery) GetOp() PairQuery_BinaryOp {
	if x != nil {
		return x.Op
	}
	return PairQuery_EQ
}

func (x *PairQuery) GetCondition() *TypedPair {
	if x != nil {
		return x.Condition
	}
	return nil
}

// QueryOrder means a Sort operation to be done for a given field.
// The key_name refers to the key of a Pair.
type QueryOrder struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	KeyName string          `protobuf:"bytes,1,opt,name=key_name,json=keyName,proto3" json:"key_name,omitempty"`
	Sort    QueryOrder_Sort `protobuf:"varint,2,opt,name=sort,proto3,enum=banyandb.v1.QueryOrder_Sort" json:"sort,omitempty"`
}

func (x *QueryOrder) Reset() {
	*x = QueryOrder{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryOrder) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryOrder) ProtoMessage() {}

func (x *QueryOrder) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[4]
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
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{4}
}

func (x *QueryOrder) GetKeyName() string {
	if x != nil {
		return x.KeyName
	}
	return ""
}

func (x *QueryOrder) GetSort() QueryOrder_Sort {
	if x != nil {
		return x.Sort
	}
	return QueryOrder_DESC
}

// Entity represents
// (Trace context) a Span defined in Google Dapper paper or equivalently a Segment in Skywalking.
// (Log context) a log
type Entity struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// entity_id could be span_id of a Span or segment_id of a Segment in the context of Trace
	EntityId string `protobuf:"bytes,1,opt,name=entity_id,json=entityId,proto3" json:"entity_id,omitempty"`
	// timestamp represents
	// 1) either the start time of a Span/Segment,
	// 2) or the timestamp of a log
	Timestamp *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// data_binary contains all un-indexed Tags and other key-value pairs
	DataBinary []byte `protobuf:"bytes,3,opt,name=data_binary,json=dataBinary,proto3" json:"data_binary,omitempty"`
	// fields contains all indexed Field. Some typical names,
	// - trace_id
	// - duration
	// - service_name
	// - service_instance_id
	// - end_time_nanoseconds
	Fields []*TypedPair `protobuf:"bytes,4,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (x *Entity) Reset() {
	*x = Entity{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entity) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entity) ProtoMessage() {}

func (x *Entity) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entity.ProtoReflect.Descriptor instead.
func (*Entity) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{5}
}

func (x *Entity) GetEntityId() string {
	if x != nil {
		return x.EntityId
	}
	return ""
}

func (x *Entity) GetTimestamp() *timestamppb.Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *Entity) GetDataBinary() []byte {
	if x != nil {
		return x.DataBinary
	}
	return nil
}

func (x *Entity) GetFields() []*TypedPair {
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

	// entities are the actual data returned
	Entities []*Entity `protobuf:"bytes,1,rep,name=entities,proto3" json:"entities,omitempty"`
}

func (x *QueryResponse) Reset() {
	*x = QueryResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryResponse) ProtoMessage() {}

func (x *QueryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[6]
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
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{6}
}

func (x *QueryResponse) GetEntities() []*Entity {
	if x != nil {
		return x.Entities
	}
	return nil
}

// Projection is used to select the names of keys to be returned.
type Projection struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The key_name refers to the key(s) of Pair(s).
	KeyNames []string `protobuf:"bytes,1,rep,name=key_names,json=keyNames,proto3" json:"key_names,omitempty"`
}

func (x *Projection) Reset() {
	*x = Projection{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Projection) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Projection) ProtoMessage() {}

func (x *Projection) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Projection.ProtoReflect.Descriptor instead.
func (*Projection) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{7}
}

func (x *Projection) GetKeyNames() []string {
	if x != nil {
		return x.KeyNames
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
		mi := &file_banyandb_v1_query_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TimeRange) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TimeRange) ProtoMessage() {}

func (x *TimeRange) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[8]
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
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{8}
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

// EntityCriteria is the request contract for query.
type EntityCriteria struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// metadata is required
	Metadata *Metadata `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	// time_range is a range query with begin/end time of entities in the timeunit of nanoseconds.
	// In the context of Trace, it represents the range of the `startTime` for spans/segments,
	// while in the context of Log, it means the range of the timestamp(s) for logs.
	// it is always recommended to specify time range for performance reason
	TimeRange *TimeRange `protobuf:"bytes,2,opt,name=time_range,json=timeRange,proto3" json:"time_range,omitempty"`
	// offset is used to support pagination, together with the following limit
	Offset uint32 `protobuf:"varint,3,opt,name=offset,proto3" json:"offset,omitempty"`
	// limit is used to impose a boundary on the number of records being returned
	Limit uint32 `protobuf:"varint,4,opt,name=limit,proto3" json:"limit,omitempty"`
	// order_by is given to specify the sort for a field. So far, only fields in the type of Integer are supported
	OrderBy *QueryOrder `protobuf:"bytes,5,opt,name=order_by,json=orderBy,proto3" json:"order_by,omitempty"`
	// fields are indexed. Some typical fields are listed below,
	// - trace_id: if given, it takes precedence over other fields and will be used to retrieve entities before other conditions are imposed
	// - duration: typical for trace context
	Fields []*PairQuery `protobuf:"bytes,6,rep,name=fields,proto3" json:"fields,omitempty"`
	// projection can be used to select the key names of the entities in the response
	Projection *Projection `protobuf:"bytes,7,opt,name=projection,proto3" json:"projection,omitempty"`
}

func (x *EntityCriteria) Reset() {
	*x = EntityCriteria{}
	if protoimpl.UnsafeEnabled {
		mi := &file_banyandb_v1_query_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EntityCriteria) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EntityCriteria) ProtoMessage() {}

func (x *EntityCriteria) ProtoReflect() protoreflect.Message {
	mi := &file_banyandb_v1_query_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EntityCriteria.ProtoReflect.Descriptor instead.
func (*EntityCriteria) Descriptor() ([]byte, []int) {
	return file_banyandb_v1_query_proto_rawDescGZIP(), []int{9}
}

func (x *EntityCriteria) GetMetadata() *Metadata {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *EntityCriteria) GetTimeRange() *TimeRange {
	if x != nil {
		return x.TimeRange
	}
	return nil
}

func (x *EntityCriteria) GetOffset() uint32 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *EntityCriteria) GetLimit() uint32 {
	if x != nil {
		return x.Limit
	}
	return 0
}

func (x *EntityCriteria) GetOrderBy() *QueryOrder {
	if x != nil {
		return x.OrderBy
	}
	return nil
}

func (x *EntityCriteria) GetFields() []*PairQuery {
	if x != nil {
		return x.Fields
	}
	return nil
}

func (x *EntityCriteria) GetProjection() *Projection {
	if x != nil {
		return x.Projection
	}
	return nil
}

var File_banyandb_v1_query_proto protoreflect.FileDescriptor

var file_banyandb_v1_query_proto_rawDesc = []byte{
	0x0a, 0x17, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x76, 0x31, 0x2f, 0x71, 0x75,
	0x65, 0x72, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x62, 0x61, 0x6e, 0x79, 0x61,
	0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1a, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64,
	0x62, 0x2f, 0x76, 0x31, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0x33, 0x0a, 0x07, 0x49, 0x6e, 0x74, 0x50, 0x61, 0x69, 0x72, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x03,
	0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22, 0x33, 0x0a, 0x07, 0x53, 0x74, 0x72, 0x50,
	0x61, 0x69, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22, 0x78, 0x0a,
	0x09, 0x54, 0x79, 0x70, 0x65, 0x64, 0x50, 0x61, 0x69, 0x72, 0x12, 0x30, 0x0a, 0x07, 0x69, 0x6e,
	0x74, 0x50, 0x61, 0x69, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x74, 0x50, 0x61, 0x69,
	0x72, 0x48, 0x00, 0x52, 0x07, 0x69, 0x6e, 0x74, 0x50, 0x61, 0x69, 0x72, 0x12, 0x30, 0x0a, 0x07,
	0x73, 0x74, 0x72, 0x50, 0x61, 0x69, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e,
	0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x50,
	0x61, 0x69, 0x72, 0x48, 0x00, 0x52, 0x07, 0x73, 0x74, 0x72, 0x50, 0x61, 0x69, 0x72, 0x42, 0x07,
	0x0a, 0x05, 0x74, 0x79, 0x70, 0x65, 0x64, 0x22, 0xca, 0x01, 0x0a, 0x09, 0x50, 0x61, 0x69, 0x72,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x12, 0x2f, 0x0a, 0x02, 0x6f, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x1f, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e,
	0x50, 0x61, 0x69, 0x72, 0x51, 0x75, 0x65, 0x72, 0x79, 0x2e, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79,
	0x4f, 0x70, 0x52, 0x02, 0x6f, 0x70, 0x12, 0x34, 0x0a, 0x09, 0x63, 0x6f, 0x6e, 0x64, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79,
	0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x79, 0x70, 0x65, 0x64, 0x50, 0x61, 0x69,
	0x72, 0x52, 0x09, 0x63, 0x6f, 0x6e, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x56, 0x0a, 0x08,
	0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x4f, 0x70, 0x12, 0x06, 0x0a, 0x02, 0x45, 0x51, 0x10, 0x00,
	0x12, 0x06, 0x0a, 0x02, 0x4e, 0x45, 0x10, 0x01, 0x12, 0x06, 0x0a, 0x02, 0x4c, 0x54, 0x10, 0x02,
	0x12, 0x06, 0x0a, 0x02, 0x47, 0x54, 0x10, 0x03, 0x12, 0x06, 0x0a, 0x02, 0x4c, 0x45, 0x10, 0x04,
	0x12, 0x06, 0x0a, 0x02, 0x47, 0x45, 0x10, 0x05, 0x12, 0x0a, 0x0a, 0x06, 0x48, 0x41, 0x56, 0x49,
	0x4e, 0x47, 0x10, 0x06, 0x12, 0x0e, 0x0a, 0x0a, 0x4e, 0x4f, 0x54, 0x5f, 0x48, 0x41, 0x56, 0x49,
	0x4e, 0x47, 0x10, 0x07, 0x22, 0x74, 0x0a, 0x0a, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4f, 0x72, 0x64,
	0x65, 0x72, 0x12, 0x19, 0x0a, 0x08, 0x6b, 0x65, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6b, 0x65, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x30, 0x0a,
	0x04, 0x73, 0x6f, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1c, 0x2e, 0x62, 0x61,
	0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4f,
	0x72, 0x64, 0x65, 0x72, 0x2e, 0x53, 0x6f, 0x72, 0x74, 0x52, 0x04, 0x73, 0x6f, 0x72, 0x74, 0x22,
	0x19, 0x0a, 0x04, 0x53, 0x6f, 0x72, 0x74, 0x12, 0x08, 0x0a, 0x04, 0x44, 0x45, 0x53, 0x43, 0x10,
	0x00, 0x12, 0x07, 0x0a, 0x03, 0x41, 0x53, 0x43, 0x10, 0x01, 0x22, 0xb0, 0x01, 0x0a, 0x06, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x79, 0x12, 0x1b, 0x0a, 0x09, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79,
	0x49, 0x64, 0x12, 0x38, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1f, 0x0a, 0x0b,
	0x64, 0x61, 0x74, 0x61, 0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x0a, 0x64, 0x61, 0x74, 0x61, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x12, 0x2e, 0x0a,
	0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e,
	0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x79, 0x70, 0x65,
	0x64, 0x50, 0x61, 0x69, 0x72, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x22, 0x40, 0x0a,
	0x0d, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2f,
	0x0a, 0x08, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x13, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x08, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x22,
	0x29, 0x0a, 0x0a, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1b, 0x0a,
	0x09, 0x6b, 0x65, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x08, 0x6b, 0x65, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x22, 0x6b, 0x0a, 0x09, 0x54, 0x69,
	0x6d, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x30, 0x0a, 0x05, 0x62, 0x65, 0x67, 0x69, 0x6e,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x52, 0x05, 0x62, 0x65, 0x67, 0x69, 0x6e, 0x12, 0x2c, 0x0a, 0x03, 0x65, 0x6e, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x52, 0x03, 0x65, 0x6e, 0x64, 0x22, 0xc5, 0x02, 0x0a, 0x0e, 0x45, 0x6e, 0x74, 0x69,
	0x74, 0x79, 0x43, 0x72, 0x69, 0x74, 0x65, 0x72, 0x69, 0x61, 0x12, 0x31, 0x0a, 0x08, 0x6d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x62,
	0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x35, 0x0a,
	0x0a, 0x74, 0x69, 0x6d, 0x65, 0x5f, 0x72, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x52,
	0x61, 0x6e, 0x67, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x14, 0x0a, 0x05,
	0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x6c, 0x69, 0x6d,
	0x69, 0x74, 0x12, 0x32, 0x0a, 0x08, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x5f, 0x62, 0x79, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e,
	0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4f, 0x72, 0x64, 0x65, 0x72, 0x52, 0x07, 0x6f,
	0x72, 0x64, 0x65, 0x72, 0x42, 0x79, 0x12, 0x2e, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73,
	0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64,
	0x62, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x61, 0x69, 0x72, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x06,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x12, 0x37, 0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x62, 0x61, 0x6e,
	0x79, 0x61, 0x6e, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x42,
	0x60, 0x0a, 0x1e, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e, 0x73, 0x6b,
	0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2e, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64,
	0x62, 0x5a, 0x3e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x70,
	0x61, 0x63, 0x68, 0x65, 0x2f, 0x73, 0x6b, 0x79, 0x77, 0x61, 0x6c, 0x6b, 0x69, 0x6e, 0x67, 0x2d,
	0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2f, 0x62, 0x61, 0x6e, 0x79, 0x61, 0x6e, 0x64, 0x62, 0x2f, 0x76, 0x31, 0x3b, 0x76,
	0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_banyandb_v1_query_proto_rawDescOnce sync.Once
	file_banyandb_v1_query_proto_rawDescData = file_banyandb_v1_query_proto_rawDesc
)

func file_banyandb_v1_query_proto_rawDescGZIP() []byte {
	file_banyandb_v1_query_proto_rawDescOnce.Do(func() {
		file_banyandb_v1_query_proto_rawDescData = protoimpl.X.CompressGZIP(file_banyandb_v1_query_proto_rawDescData)
	})
	return file_banyandb_v1_query_proto_rawDescData
}

var file_banyandb_v1_query_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_banyandb_v1_query_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_banyandb_v1_query_proto_goTypes = []interface{}{
	(PairQuery_BinaryOp)(0),       // 0: banyandb.v1.PairQuery.BinaryOp
	(QueryOrder_Sort)(0),          // 1: banyandb.v1.QueryOrder.Sort
	(*IntPair)(nil),               // 2: banyandb.v1.IntPair
	(*StrPair)(nil),               // 3: banyandb.v1.StrPair
	(*TypedPair)(nil),             // 4: banyandb.v1.TypedPair
	(*PairQuery)(nil),             // 5: banyandb.v1.PairQuery
	(*QueryOrder)(nil),            // 6: banyandb.v1.QueryOrder
	(*Entity)(nil),                // 7: banyandb.v1.Entity
	(*QueryResponse)(nil),         // 8: banyandb.v1.QueryResponse
	(*Projection)(nil),            // 9: banyandb.v1.Projection
	(*TimeRange)(nil),             // 10: banyandb.v1.TimeRange
	(*EntityCriteria)(nil),        // 11: banyandb.v1.EntityCriteria
	(*timestamppb.Timestamp)(nil), // 12: google.protobuf.Timestamp
	(*Metadata)(nil),              // 13: banyandb.v1.Metadata
}
var file_banyandb_v1_query_proto_depIdxs = []int32{
	2,  // 0: banyandb.v1.TypedPair.intPair:type_name -> banyandb.v1.IntPair
	3,  // 1: banyandb.v1.TypedPair.strPair:type_name -> banyandb.v1.StrPair
	0,  // 2: banyandb.v1.PairQuery.op:type_name -> banyandb.v1.PairQuery.BinaryOp
	4,  // 3: banyandb.v1.PairQuery.condition:type_name -> banyandb.v1.TypedPair
	1,  // 4: banyandb.v1.QueryOrder.sort:type_name -> banyandb.v1.QueryOrder.Sort
	12, // 5: banyandb.v1.Entity.timestamp:type_name -> google.protobuf.Timestamp
	4,  // 6: banyandb.v1.Entity.fields:type_name -> banyandb.v1.TypedPair
	7,  // 7: banyandb.v1.QueryResponse.entities:type_name -> banyandb.v1.Entity
	12, // 8: banyandb.v1.TimeRange.begin:type_name -> google.protobuf.Timestamp
	12, // 9: banyandb.v1.TimeRange.end:type_name -> google.protobuf.Timestamp
	13, // 10: banyandb.v1.EntityCriteria.metadata:type_name -> banyandb.v1.Metadata
	10, // 11: banyandb.v1.EntityCriteria.time_range:type_name -> banyandb.v1.TimeRange
	6,  // 12: banyandb.v1.EntityCriteria.order_by:type_name -> banyandb.v1.QueryOrder
	5,  // 13: banyandb.v1.EntityCriteria.fields:type_name -> banyandb.v1.PairQuery
	9,  // 14: banyandb.v1.EntityCriteria.projection:type_name -> banyandb.v1.Projection
	15, // [15:15] is the sub-list for method output_type
	15, // [15:15] is the sub-list for method input_type
	15, // [15:15] is the sub-list for extension type_name
	15, // [15:15] is the sub-list for extension extendee
	0,  // [0:15] is the sub-list for field type_name
}

func init() { file_banyandb_v1_query_proto_init() }
func file_banyandb_v1_query_proto_init() {
	if File_banyandb_v1_query_proto != nil {
		return
	}
	file_banyandb_v1_database_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_banyandb_v1_query_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IntPair); i {
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
		file_banyandb_v1_query_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StrPair); i {
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
		file_banyandb_v1_query_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TypedPair); i {
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
		file_banyandb_v1_query_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PairQuery); i {
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
		file_banyandb_v1_query_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_v1_query_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entity); i {
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
		file_banyandb_v1_query_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_v1_query_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Projection); i {
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
		file_banyandb_v1_query_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
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
		file_banyandb_v1_query_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EntityCriteria); i {
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
	file_banyandb_v1_query_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*TypedPair_IntPair)(nil),
		(*TypedPair_StrPair)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_banyandb_v1_query_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_banyandb_v1_query_proto_goTypes,
		DependencyIndexes: file_banyandb_v1_query_proto_depIdxs,
		EnumInfos:         file_banyandb_v1_query_proto_enumTypes,
		MessageInfos:      file_banyandb_v1_query_proto_msgTypes,
	}.Build()
	File_banyandb_v1_query_proto = out.File
	file_banyandb_v1_query_proto_rawDesc = nil
	file_banyandb_v1_query_proto_goTypes = nil
	file_banyandb_v1_query_proto_depIdxs = nil
}
