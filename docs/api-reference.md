# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/cluster/v1/rpc.proto](#banyandb_cluster_v1_rpc-proto)
    - [SendRequest](#banyandb-cluster-v1-SendRequest)
    - [SendResponse](#banyandb-cluster-v1-SendResponse)
  
    - [Service](#banyandb-cluster-v1-Service)
  
- [banyandb/common/v1/common.proto](#banyandb_common_v1_common-proto)
    - [Group](#banyandb-common-v1-Group)
    - [IntervalRule](#banyandb-common-v1-IntervalRule)
    - [Metadata](#banyandb-common-v1-Metadata)
    - [ResourceOpts](#banyandb-common-v1-ResourceOpts)
  
    - [Catalog](#banyandb-common-v1-Catalog)
    - [IntervalRule.Unit](#banyandb-common-v1-IntervalRule-Unit)
  
- [banyandb/common/v1/trace.proto](#banyandb_common_v1_trace-proto)
    - [Span](#banyandb-common-v1-Span)
    - [Tag](#banyandb-common-v1-Tag)
    - [Trace](#banyandb-common-v1-Trace)
  
- [banyandb/database/v1/database.proto](#banyandb_database_v1_database-proto)
    - [Node](#banyandb-database-v1-Node)
    - [Shard](#banyandb-database-v1-Shard)
  
    - [Role](#banyandb-database-v1-Role)
  
- [banyandb/model/v1/common.proto](#banyandb_model_v1_common-proto)
    - [FieldValue](#banyandb-model-v1-FieldValue)
    - [Float](#banyandb-model-v1-Float)
    - [Int](#banyandb-model-v1-Int)
    - [IntArray](#banyandb-model-v1-IntArray)
    - [Str](#banyandb-model-v1-Str)
    - [StrArray](#banyandb-model-v1-StrArray)
    - [TagFamilyForWrite](#banyandb-model-v1-TagFamilyForWrite)
    - [TagValue](#banyandb-model-v1-TagValue)
  
    - [AggregationFunction](#banyandb-model-v1-AggregationFunction)
  
- [banyandb/model/v1/query.proto](#banyandb_model_v1_query-proto)
    - [Condition](#banyandb-model-v1-Condition)
    - [Criteria](#banyandb-model-v1-Criteria)
    - [LogicalExpression](#banyandb-model-v1-LogicalExpression)
    - [QueryOrder](#banyandb-model-v1-QueryOrder)
    - [Tag](#banyandb-model-v1-Tag)
    - [TagFamily](#banyandb-model-v1-TagFamily)
    - [TagProjection](#banyandb-model-v1-TagProjection)
    - [TagProjection.TagFamily](#banyandb-model-v1-TagProjection-TagFamily)
    - [TimeRange](#banyandb-model-v1-TimeRange)
  
    - [Condition.BinaryOp](#banyandb-model-v1-Condition-BinaryOp)
    - [LogicalExpression.LogicalOp](#banyandb-model-v1-LogicalExpression-LogicalOp)
    - [Sort](#banyandb-model-v1-Sort)
  
- [banyandb/database/v1/schema.proto](#banyandb_database_v1_schema-proto)
    - [Entity](#banyandb-database-v1-Entity)
    - [FieldSpec](#banyandb-database-v1-FieldSpec)
    - [IndexRule](#banyandb-database-v1-IndexRule)
    - [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding)
    - [Measure](#banyandb-database-v1-Measure)
    - [Stream](#banyandb-database-v1-Stream)
    - [Subject](#banyandb-database-v1-Subject)
    - [TagFamilySpec](#banyandb-database-v1-TagFamilySpec)
    - [TagSpec](#banyandb-database-v1-TagSpec)
    - [TopNAggregation](#banyandb-database-v1-TopNAggregation)
  
    - [CompressionMethod](#banyandb-database-v1-CompressionMethod)
    - [EncodingMethod](#banyandb-database-v1-EncodingMethod)
    - [FieldType](#banyandb-database-v1-FieldType)
    - [IndexRule.Analyzer](#banyandb-database-v1-IndexRule-Analyzer)
    - [IndexRule.Type](#banyandb-database-v1-IndexRule-Type)
    - [TagType](#banyandb-database-v1-TagType)
  
- [banyandb/database/v1/rpc.proto](#banyandb_database_v1_rpc-proto)
    - [GroupRegistryServiceCreateRequest](#banyandb-database-v1-GroupRegistryServiceCreateRequest)
    - [GroupRegistryServiceCreateResponse](#banyandb-database-v1-GroupRegistryServiceCreateResponse)
    - [GroupRegistryServiceDeleteRequest](#banyandb-database-v1-GroupRegistryServiceDeleteRequest)
    - [GroupRegistryServiceDeleteResponse](#banyandb-database-v1-GroupRegistryServiceDeleteResponse)
    - [GroupRegistryServiceExistRequest](#banyandb-database-v1-GroupRegistryServiceExistRequest)
    - [GroupRegistryServiceExistResponse](#banyandb-database-v1-GroupRegistryServiceExistResponse)
    - [GroupRegistryServiceGetRequest](#banyandb-database-v1-GroupRegistryServiceGetRequest)
    - [GroupRegistryServiceGetResponse](#banyandb-database-v1-GroupRegistryServiceGetResponse)
    - [GroupRegistryServiceListRequest](#banyandb-database-v1-GroupRegistryServiceListRequest)
    - [GroupRegistryServiceListResponse](#banyandb-database-v1-GroupRegistryServiceListResponse)
    - [GroupRegistryServiceUpdateRequest](#banyandb-database-v1-GroupRegistryServiceUpdateRequest)
    - [GroupRegistryServiceUpdateResponse](#banyandb-database-v1-GroupRegistryServiceUpdateResponse)
    - [IndexRuleBindingRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest)
    - [IndexRuleBindingRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse)
    - [IndexRuleBindingRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest)
    - [IndexRuleBindingRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse)
    - [IndexRuleBindingRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistRequest)
    - [IndexRuleBindingRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistResponse)
    - [IndexRuleBindingRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest)
    - [IndexRuleBindingRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse)
    - [IndexRuleBindingRegistryServiceListRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest)
    - [IndexRuleBindingRegistryServiceListResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse)
    - [IndexRuleBindingRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest)
    - [IndexRuleBindingRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse)
    - [IndexRuleRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleRegistryServiceCreateRequest)
    - [IndexRuleRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleRegistryServiceCreateResponse)
    - [IndexRuleRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest)
    - [IndexRuleRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse)
    - [IndexRuleRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleRegistryServiceExistRequest)
    - [IndexRuleRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleRegistryServiceExistResponse)
    - [IndexRuleRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleRegistryServiceGetRequest)
    - [IndexRuleRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleRegistryServiceGetResponse)
    - [IndexRuleRegistryServiceListRequest](#banyandb-database-v1-IndexRuleRegistryServiceListRequest)
    - [IndexRuleRegistryServiceListResponse](#banyandb-database-v1-IndexRuleRegistryServiceListResponse)
    - [IndexRuleRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest)
    - [IndexRuleRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse)
    - [MeasureRegistryServiceCreateRequest](#banyandb-database-v1-MeasureRegistryServiceCreateRequest)
    - [MeasureRegistryServiceCreateResponse](#banyandb-database-v1-MeasureRegistryServiceCreateResponse)
    - [MeasureRegistryServiceDeleteRequest](#banyandb-database-v1-MeasureRegistryServiceDeleteRequest)
    - [MeasureRegistryServiceDeleteResponse](#banyandb-database-v1-MeasureRegistryServiceDeleteResponse)
    - [MeasureRegistryServiceExistRequest](#banyandb-database-v1-MeasureRegistryServiceExistRequest)
    - [MeasureRegistryServiceExistResponse](#banyandb-database-v1-MeasureRegistryServiceExistResponse)
    - [MeasureRegistryServiceGetRequest](#banyandb-database-v1-MeasureRegistryServiceGetRequest)
    - [MeasureRegistryServiceGetResponse](#banyandb-database-v1-MeasureRegistryServiceGetResponse)
    - [MeasureRegistryServiceListRequest](#banyandb-database-v1-MeasureRegistryServiceListRequest)
    - [MeasureRegistryServiceListResponse](#banyandb-database-v1-MeasureRegistryServiceListResponse)
    - [MeasureRegistryServiceUpdateRequest](#banyandb-database-v1-MeasureRegistryServiceUpdateRequest)
    - [MeasureRegistryServiceUpdateResponse](#banyandb-database-v1-MeasureRegistryServiceUpdateResponse)
    - [StreamRegistryServiceCreateRequest](#banyandb-database-v1-StreamRegistryServiceCreateRequest)
    - [StreamRegistryServiceCreateResponse](#banyandb-database-v1-StreamRegistryServiceCreateResponse)
    - [StreamRegistryServiceDeleteRequest](#banyandb-database-v1-StreamRegistryServiceDeleteRequest)
    - [StreamRegistryServiceDeleteResponse](#banyandb-database-v1-StreamRegistryServiceDeleteResponse)
    - [StreamRegistryServiceExistRequest](#banyandb-database-v1-StreamRegistryServiceExistRequest)
    - [StreamRegistryServiceExistResponse](#banyandb-database-v1-StreamRegistryServiceExistResponse)
    - [StreamRegistryServiceGetRequest](#banyandb-database-v1-StreamRegistryServiceGetRequest)
    - [StreamRegistryServiceGetResponse](#banyandb-database-v1-StreamRegistryServiceGetResponse)
    - [StreamRegistryServiceListRequest](#banyandb-database-v1-StreamRegistryServiceListRequest)
    - [StreamRegistryServiceListResponse](#banyandb-database-v1-StreamRegistryServiceListResponse)
    - [StreamRegistryServiceUpdateRequest](#banyandb-database-v1-StreamRegistryServiceUpdateRequest)
    - [StreamRegistryServiceUpdateResponse](#banyandb-database-v1-StreamRegistryServiceUpdateResponse)
    - [TopNAggregationRegistryServiceCreateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest)
    - [TopNAggregationRegistryServiceCreateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse)
    - [TopNAggregationRegistryServiceDeleteRequest](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest)
    - [TopNAggregationRegistryServiceDeleteResponse](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse)
    - [TopNAggregationRegistryServiceExistRequest](#banyandb-database-v1-TopNAggregationRegistryServiceExistRequest)
    - [TopNAggregationRegistryServiceExistResponse](#banyandb-database-v1-TopNAggregationRegistryServiceExistResponse)
    - [TopNAggregationRegistryServiceGetRequest](#banyandb-database-v1-TopNAggregationRegistryServiceGetRequest)
    - [TopNAggregationRegistryServiceGetResponse](#banyandb-database-v1-TopNAggregationRegistryServiceGetResponse)
    - [TopNAggregationRegistryServiceListRequest](#banyandb-database-v1-TopNAggregationRegistryServiceListRequest)
    - [TopNAggregationRegistryServiceListResponse](#banyandb-database-v1-TopNAggregationRegistryServiceListResponse)
    - [TopNAggregationRegistryServiceUpdateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest)
    - [TopNAggregationRegistryServiceUpdateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse)
  
    - [GroupRegistryService](#banyandb-database-v1-GroupRegistryService)
    - [IndexRuleBindingRegistryService](#banyandb-database-v1-IndexRuleBindingRegistryService)
    - [IndexRuleRegistryService](#banyandb-database-v1-IndexRuleRegistryService)
    - [MeasureRegistryService](#banyandb-database-v1-MeasureRegistryService)
    - [StreamRegistryService](#banyandb-database-v1-StreamRegistryService)
    - [TopNAggregationRegistryService](#banyandb-database-v1-TopNAggregationRegistryService)
  
- [banyandb/measure/v1/query.proto](#banyandb_measure_v1_query-proto)
    - [DataPoint](#banyandb-measure-v1-DataPoint)
    - [DataPoint.Field](#banyandb-measure-v1-DataPoint-Field)
    - [QueryRequest](#banyandb-measure-v1-QueryRequest)
    - [QueryRequest.Aggregation](#banyandb-measure-v1-QueryRequest-Aggregation)
    - [QueryRequest.FieldProjection](#banyandb-measure-v1-QueryRequest-FieldProjection)
    - [QueryRequest.GroupBy](#banyandb-measure-v1-QueryRequest-GroupBy)
    - [QueryRequest.Top](#banyandb-measure-v1-QueryRequest-Top)
    - [QueryResponse](#banyandb-measure-v1-QueryResponse)
  
- [banyandb/measure/v1/topn.proto](#banyandb_measure_v1_topn-proto)
    - [TopNList](#banyandb-measure-v1-TopNList)
    - [TopNList.Item](#banyandb-measure-v1-TopNList-Item)
    - [TopNRequest](#banyandb-measure-v1-TopNRequest)
    - [TopNResponse](#banyandb-measure-v1-TopNResponse)
  
- [banyandb/model/v1/write.proto](#banyandb_model_v1_write-proto)
    - [Status](#banyandb-model-v1-Status)
  
- [banyandb/measure/v1/write.proto](#banyandb_measure_v1_write-proto)
    - [DataPointValue](#banyandb-measure-v1-DataPointValue)
    - [InternalWriteRequest](#banyandb-measure-v1-InternalWriteRequest)
    - [WriteRequest](#banyandb-measure-v1-WriteRequest)
    - [WriteResponse](#banyandb-measure-v1-WriteResponse)
  
- [banyandb/measure/v1/rpc.proto](#banyandb_measure_v1_rpc-proto)
    - [MeasureService](#banyandb-measure-v1-MeasureService)
  
- [banyandb/property/v1/property.proto](#banyandb_property_v1_property-proto)
    - [Metadata](#banyandb-property-v1-Metadata)
    - [Property](#banyandb-property-v1-Property)
  
- [banyandb/property/v1/rpc.proto](#banyandb_property_v1_rpc-proto)
    - [ApplyRequest](#banyandb-property-v1-ApplyRequest)
    - [ApplyResponse](#banyandb-property-v1-ApplyResponse)
    - [DeleteRequest](#banyandb-property-v1-DeleteRequest)
    - [DeleteResponse](#banyandb-property-v1-DeleteResponse)
    - [GetRequest](#banyandb-property-v1-GetRequest)
    - [GetResponse](#banyandb-property-v1-GetResponse)
    - [KeepAliveRequest](#banyandb-property-v1-KeepAliveRequest)
    - [KeepAliveResponse](#banyandb-property-v1-KeepAliveResponse)
    - [ListRequest](#banyandb-property-v1-ListRequest)
    - [ListResponse](#banyandb-property-v1-ListResponse)
  
    - [ApplyRequest.Strategy](#banyandb-property-v1-ApplyRequest-Strategy)
  
    - [PropertyService](#banyandb-property-v1-PropertyService)
  
- [banyandb/stream/v1/query.proto](#banyandb_stream_v1_query-proto)
    - [Element](#banyandb-stream-v1-Element)
    - [QueryRequest](#banyandb-stream-v1-QueryRequest)
    - [QueryResponse](#banyandb-stream-v1-QueryResponse)
  
- [banyandb/stream/v1/write.proto](#banyandb_stream_v1_write-proto)
    - [ElementValue](#banyandb-stream-v1-ElementValue)
    - [InternalWriteRequest](#banyandb-stream-v1-InternalWriteRequest)
    - [WriteRequest](#banyandb-stream-v1-WriteRequest)
    - [WriteResponse](#banyandb-stream-v1-WriteResponse)
  
- [banyandb/stream/v1/rpc.proto](#banyandb_stream_v1_rpc-proto)
    - [StreamService](#banyandb-stream-v1-StreamService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="banyandb_cluster_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/cluster/v1/rpc.proto



<a name="banyandb-cluster-v1-SendRequest"></a>

### SendRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| topic | [string](#string) |  |  |
| message_id | [uint64](#uint64) |  |  |
| body | [google.protobuf.Any](#google-protobuf-Any) |  |  |
| batch_mod | [bool](#bool) |  |  |






<a name="banyandb-cluster-v1-SendResponse"></a>

### SendResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message_id | [uint64](#uint64) |  |  |
| error | [string](#string) |  |  |
| body | [google.protobuf.Any](#google-protobuf-Any) |  |  |





 

 

 


<a name="banyandb-cluster-v1-Service"></a>

### Service


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Send | [SendRequest](#banyandb-cluster-v1-SendRequest) stream | [SendResponse](#banyandb-cluster-v1-SendResponse) stream |  |

 



<a name="banyandb_common_v1_common-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/common/v1/common.proto



<a name="banyandb-common-v1-Group"></a>

### Group
Group is an internal object for Group management


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-common-v1-Metadata) |  | metadata define the group&#39;s identity |
| catalog | [Catalog](#banyandb-common-v1-Catalog) |  | catalog denotes which type of data the group contains |
| resource_opts | [ResourceOpts](#banyandb-common-v1-ResourceOpts) |  | resourceOpts indicates the structure of the underlying kv storage |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when resources of the group are updated |






<a name="banyandb-common-v1-IntervalRule"></a>

### IntervalRule
IntervalRule is a structured duration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unit | [IntervalRule.Unit](#banyandb-common-v1-IntervalRule-Unit) |  | unit can only be UNIT_HOUR or UNIT_DAY |
| num | [uint32](#uint32) |  |  |






<a name="banyandb-common-v1-Metadata"></a>

### Metadata
Metadata is for multi-tenant, multi-model use


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | group contains a set of options, like retention policy, max |
| name | [string](#string) |  | name of the entity |
| id | [uint32](#uint32) |  | id is the unique identifier of the entity if id is not set, the system will generate a unique id |
| create_revision | [int64](#int64) |  | readonly. create_revision is the revision of last creation on this key. |
| mod_revision | [int64](#int64) |  | readonly. mod_revision is the revision of last modification on this key. |






<a name="banyandb-common-v1-ResourceOpts"></a>

### ResourceOpts



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_num | [uint32](#uint32) |  | shard_num is the number of shards |
| segment_interval | [IntervalRule](#banyandb-common-v1-IntervalRule) |  | segment_interval indicates the length of a segment |
| ttl | [IntervalRule](#banyandb-common-v1-IntervalRule) |  | ttl indicates time to live, how long the data will be cached |





 


<a name="banyandb-common-v1-Catalog"></a>

### Catalog


| Name | Number | Description |
| ---- | ------ | ----------- |
| CATALOG_UNSPECIFIED | 0 |  |
| CATALOG_STREAM | 1 |  |
| CATALOG_MEASURE | 2 |  |



<a name="banyandb-common-v1-IntervalRule-Unit"></a>

### IntervalRule.Unit


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNIT_UNSPECIFIED | 0 |  |
| UNIT_HOUR | 1 |  |
| UNIT_DAY | 2 |  |


 

 

 



<a name="banyandb_common_v1_trace-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/common/v1/trace.proto



<a name="banyandb-common-v1-Span"></a>

### Span
Span is the basic unit of a trace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | start_time is the start time of the span. |
| end_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | end_time is the end time of the span. |
| error | [bool](#bool) |  | error indicates whether the span is an error span. |
| tags | [Tag](#banyandb-common-v1-Tag) | repeated | tags is a list of tags of the span. |
| message | [string](#string) |  | message is the message generated by the span. |
| children | [Span](#banyandb-common-v1-Span) | repeated | children is a list of child spans of the span. |






<a name="banyandb-common-v1-Tag"></a>

### Tag
Tag is the key-value pair of a span.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | key is the key of the tag. |
| value | [string](#string) |  | value is the value of the tag. |






<a name="banyandb-common-v1-Trace"></a>

### Trace
Trace is the top level message of a trace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trace_id | [string](#string) |  | trace_id is the unique identifier of the trace. |
| spans | [Span](#banyandb-common-v1-Span) | repeated | spans is a list of spans in the trace. |
| error | [bool](#bool) |  | error indicates whether the trace is an error trace. |





 

 

 

 



<a name="banyandb_database_v1_database-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/database.proto



<a name="banyandb-database-v1-Node"></a>

### Node



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| roles | [Role](#banyandb-database-v1-Role) | repeated |  |
| grpc_address | [string](#string) |  |  |
| http_address | [string](#string) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="banyandb-database-v1-Shard"></a>

### Shard



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  |  |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  |  |
| node | [string](#string) |  |  |
| total | [uint32](#uint32) |  |  |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 


<a name="banyandb-database-v1-Role"></a>

### Role


| Name | Number | Description |
| ---- | ------ | ----------- |
| ROLE_UNSPECIFIED | 0 |  |
| ROLE_META | 1 |  |
| ROLE_DATA | 2 |  |
| ROLE_LIAISON | 3 |  |


 

 

 



<a name="banyandb_model_v1_common-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/model/v1/common.proto



<a name="banyandb-model-v1-FieldValue"></a>

### FieldValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| null | [google.protobuf.NullValue](#google-protobuf-NullValue) |  |  |
| str | [Str](#banyandb-model-v1-Str) |  |  |
| int | [Int](#banyandb-model-v1-Int) |  |  |
| binary_data | [bytes](#bytes) |  |  |
| float | [Float](#banyandb-model-v1-Float) |  |  |






<a name="banyandb-model-v1-Float"></a>

### Float



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [double](#double) |  |  |






<a name="banyandb-model-v1-Int"></a>

### Int



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="banyandb-model-v1-IntArray"></a>

### IntArray



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) | repeated |  |






<a name="banyandb-model-v1-Str"></a>

### Str



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="banyandb-model-v1-StrArray"></a>

### StrArray



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | repeated |  |






<a name="banyandb-model-v1-TagFamilyForWrite"></a>

### TagFamilyForWrite



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tags | [TagValue](#banyandb-model-v1-TagValue) | repeated |  |






<a name="banyandb-model-v1-TagValue"></a>

### TagValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| null | [google.protobuf.NullValue](#google-protobuf-NullValue) |  |  |
| str | [Str](#banyandb-model-v1-Str) |  |  |
| str_array | [StrArray](#banyandb-model-v1-StrArray) |  |  |
| int | [Int](#banyandb-model-v1-Int) |  |  |
| int_array | [IntArray](#banyandb-model-v1-IntArray) |  |  |
| binary_data | [bytes](#bytes) |  |  |





 


<a name="banyandb-model-v1-AggregationFunction"></a>

### AggregationFunction


| Name | Number | Description |
| ---- | ------ | ----------- |
| AGGREGATION_FUNCTION_UNSPECIFIED | 0 |  |
| AGGREGATION_FUNCTION_MEAN | 1 |  |
| AGGREGATION_FUNCTION_MAX | 2 |  |
| AGGREGATION_FUNCTION_MIN | 3 |  |
| AGGREGATION_FUNCTION_COUNT | 4 |  |
| AGGREGATION_FUNCTION_SUM | 5 |  |


 

 

 



<a name="banyandb_model_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/model/v1/query.proto



<a name="banyandb-model-v1-Condition"></a>

### Condition
Condition consists of the query condition with a single binary operator to be imposed
For 1:1 BinaryOp, values in condition must be an array with length = 1,
while for 1:N BinaryOp, values can be an array with length &gt;= 1.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| op | [Condition.BinaryOp](#banyandb-model-v1-Condition-BinaryOp) |  |  |
| value | [TagValue](#banyandb-model-v1-TagValue) |  |  |






<a name="banyandb-model-v1-Criteria"></a>

### Criteria
tag_families are indexed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| le | [LogicalExpression](#banyandb-model-v1-LogicalExpression) |  |  |
| condition | [Condition](#banyandb-model-v1-Condition) |  |  |






<a name="banyandb-model-v1-LogicalExpression"></a>

### LogicalExpression
LogicalExpression supports logical operation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| op | [LogicalExpression.LogicalOp](#banyandb-model-v1-LogicalExpression-LogicalOp) |  | op is a logical operation |
| left | [Criteria](#banyandb-model-v1-Criteria) |  |  |
| right | [Criteria](#banyandb-model-v1-Criteria) |  |  |






<a name="banyandb-model-v1-QueryOrder"></a>

### QueryOrder
QueryOrder means a Sort operation to be done for a given index rule.
The index_rule_name refers to the name of a index rule bound to the subject.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_name | [string](#string) |  |  |
| sort | [Sort](#banyandb-model-v1-Sort) |  |  |






<a name="banyandb-model-v1-Tag"></a>

### Tag
Pair is the building block of a record which is equivalent to a key-value pair.
In the context of Trace, it could be metadata of a trace such as service_name, service_instance, etc.
Besides, other tags are organized in key-value pair in the underlying storage layer.
One should notice that the values can be a multi-value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TagValue](#banyandb-model-v1-TagValue) |  |  |






<a name="banyandb-model-v1-TagFamily"></a>

### TagFamily



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| tags | [Tag](#banyandb-model-v1-Tag) | repeated |  |






<a name="banyandb-model-v1-TagProjection"></a>

### TagProjection
TagProjection is used to select the names of keys to be returned.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_families | [TagProjection.TagFamily](#banyandb-model-v1-TagProjection-TagFamily) | repeated |  |






<a name="banyandb-model-v1-TagProjection-TagFamily"></a>

### TagProjection.TagFamily



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| tags | [string](#string) | repeated |  |






<a name="banyandb-model-v1-TimeRange"></a>

### TimeRange
TimeRange is a range query for uint64,
the range here follows left-inclusive and right-exclusive rule, i.e. [begin, end) if both edges exist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| begin | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 


<a name="banyandb-model-v1-Condition-BinaryOp"></a>

### Condition.BinaryOp
BinaryOp specifies the operation imposed to the given query condition
For EQ, NE, LT, GT, LE and GE, only one operand should be given, i.e. one-to-one relationship.
HAVING and NOT_HAVING allow multi-value to be the operand such as array/vector, i.e. one-to-many relationship.
For example, &#34;keyA&#34; contains &#34;valueA&#34; **and** &#34;valueB&#34;
MATCH performances a full-text search if the tag is analyzed.
The string value applies to the same analyzer as the tag, but string array value does not.
Each item in a string array is seen as a token instead of a query expression.

| Name | Number | Description |
| ---- | ------ | ----------- |
| BINARY_OP_UNSPECIFIED | 0 |  |
| BINARY_OP_EQ | 1 |  |
| BINARY_OP_NE | 2 |  |
| BINARY_OP_LT | 3 |  |
| BINARY_OP_GT | 4 |  |
| BINARY_OP_LE | 5 |  |
| BINARY_OP_GE | 6 |  |
| BINARY_OP_HAVING | 7 |  |
| BINARY_OP_NOT_HAVING | 8 |  |
| BINARY_OP_IN | 9 |  |
| BINARY_OP_NOT_IN | 10 |  |
| BINARY_OP_MATCH | 11 |  |



<a name="banyandb-model-v1-LogicalExpression-LogicalOp"></a>

### LogicalExpression.LogicalOp


| Name | Number | Description |
| ---- | ------ | ----------- |
| LOGICAL_OP_UNSPECIFIED | 0 |  |
| LOGICAL_OP_AND | 1 |  |
| LOGICAL_OP_OR | 2 |  |



<a name="banyandb-model-v1-Sort"></a>

### Sort


| Name | Number | Description |
| ---- | ------ | ----------- |
| SORT_UNSPECIFIED | 0 |  |
| SORT_DESC | 1 |  |
| SORT_ASC | 2 |  |


 

 

 



<a name="banyandb_database_v1_schema-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/schema.proto



<a name="banyandb-database-v1-Entity"></a>

### Entity



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_names | [string](#string) | repeated |  |






<a name="banyandb-database-v1-FieldSpec"></a>

### FieldSpec
FieldSpec is the specification of field


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name is the identity of a field |
| field_type | [FieldType](#banyandb-database-v1-FieldType) |  | field_type denotes the type of field value |
| encoding_method | [EncodingMethod](#banyandb-database-v1-EncodingMethod) |  | encoding_method indicates how to encode data during writing |
| compression_method | [CompressionMethod](#banyandb-database-v1-CompressionMethod) |  | compression_method indicates how to compress data during writing |






<a name="banyandb-database-v1-IndexRule"></a>

### IndexRule
IndexRule defines how to generate indices based on tags and the index type
IndexRule should bind to a subject through an IndexRuleBinding to generate proper indices.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata define the rule&#39;s identity |
| tags | [string](#string) | repeated | tags are the combination that refers to an indexed object If the elements in tags are more than 1, the object will generate a multi-tag index Caveat: All tags in a multi-tag MUST have an identical IndexType |
| type | [IndexRule.Type](#banyandb-database-v1-IndexRule-Type) |  | type is the IndexType of this IndexObject. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the IndexRule is updated |
| analyzer | [IndexRule.Analyzer](#banyandb-database-v1-IndexRule-Analyzer) |  | analyzer analyzes tag value to support the full-text searching for TYPE_INVERTED indices. |






<a name="banyandb-database-v1-IndexRuleBinding"></a>

### IndexRuleBinding
IndexRuleBinding is a bridge to connect severalIndexRules to a subject
This binding is valid between begin_at_nanoseconds and expire_at_nanoseconds, that provides flexible strategies
to control how to generate time series indices.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of this binding |
| rules | [string](#string) | repeated | rules refers to the IndexRule |
| subject | [Subject](#banyandb-database-v1-Subject) |  | subject indicates the subject of binding action |
| begin_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | begin_at_nanoseconds is the timestamp, after which the binding will be active |
| expire_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | expire_at_nanoseconds it the timestamp, after which the binding will be inactive expire_at_nanoseconds must be larger than begin_at_nanoseconds |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the IndexRuleBinding is updated |






<a name="banyandb-database-v1-Measure"></a>

### Measure
Measure intends to store data point


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of a measure |
| tag_families | [TagFamilySpec](#banyandb-database-v1-TagFamilySpec) | repeated | tag_families are for filter measures |
| fields | [FieldSpec](#banyandb-database-v1-FieldSpec) | repeated | fields denote measure values |
| entity | [Entity](#banyandb-database-v1-Entity) |  | entity indicates which tags will be to generate a series and shard a measure |
| interval | [string](#string) |  | interval indicates how frequently to send a data point valid time units are &#34;ns&#34;, &#34;us&#34; (or &#34;µs&#34;), &#34;ms&#34;, &#34;s&#34;, &#34;m&#34;, &#34;h&#34;, &#34;d&#34;. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the measure is updated |






<a name="banyandb-database-v1-Stream"></a>

### Stream
Stream intends to store streaming data, for example, traces or logs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of a trace series |
| tag_families | [TagFamilySpec](#banyandb-database-v1-TagFamilySpec) | repeated | tag_families |
| entity | [Entity](#banyandb-database-v1-Entity) |  | entity indicates how to generate a series and shard a stream |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the stream is updated |






<a name="banyandb-database-v1-Subject"></a>

### Subject
Subject defines which stream or measure would generate indices


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  | catalog is where the subject belongs to todo validate plugin exist bug https://github.com/bufbuild/protoc-gen-validate/issues/672 |
| name | [string](#string) |  | name refers to a stream or measure in a particular catalog |






<a name="banyandb-database-v1-TagFamilySpec"></a>

### TagFamilySpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| tags | [TagSpec](#banyandb-database-v1-TagSpec) | repeated | tags defines accepted tags |






<a name="banyandb-database-v1-TagSpec"></a>

### TagSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [TagType](#banyandb-database-v1-TagType) |  |  |
| indexed_only | [bool](#bool) |  | indexed_only indicates whether the tag is stored True: It&#39;s indexed only, but not stored False: it&#39;s stored and indexed |






<a name="banyandb-database-v1-TopNAggregation"></a>

### TopNAggregation
TopNAggregation generates offline TopN statistics for a measure&#39;s TopN approximation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of an aggregation |
| source_measure | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | source_measure denotes the data source of this aggregation |
| field_name | [string](#string) |  | field_name is the name of field used for ranking |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields ASC: bottomN DESC: topN UNSPECIFIED: topN &#43; bottomN todo validate plugin exist bug https://github.com/bufbuild/protoc-gen-validate/issues/672 |
| group_by_tag_names | [string](#string) | repeated | group_by_tag_names groups data points into statistical counters |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | criteria select partial data points from measure |
| counters_number | [int32](#int32) |  | counters_number sets the number of counters to be tracked. The default value is 1000 |
| lru_size | [int32](#int32) |  | lru_size defines how much entry is allowed to be maintained in the memory |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the measure is updated |





 


<a name="banyandb-database-v1-CompressionMethod"></a>

### CompressionMethod


| Name | Number | Description |
| ---- | ------ | ----------- |
| COMPRESSION_METHOD_UNSPECIFIED | 0 |  |
| COMPRESSION_METHOD_ZSTD | 1 |  |



<a name="banyandb-database-v1-EncodingMethod"></a>

### EncodingMethod


| Name | Number | Description |
| ---- | ------ | ----------- |
| ENCODING_METHOD_UNSPECIFIED | 0 |  |
| ENCODING_METHOD_GORILLA | 1 |  |



<a name="banyandb-database-v1-FieldType"></a>

### FieldType


| Name | Number | Description |
| ---- | ------ | ----------- |
| FIELD_TYPE_UNSPECIFIED | 0 |  |
| FIELD_TYPE_STRING | 1 |  |
| FIELD_TYPE_INT | 2 |  |
| FIELD_TYPE_DATA_BINARY | 3 |  |
| FIELD_TYPE_FLOAT | 4 |  |



<a name="banyandb-database-v1-IndexRule-Analyzer"></a>

### IndexRule.Analyzer


| Name | Number | Description |
| ---- | ------ | ----------- |
| ANALYZER_UNSPECIFIED | 0 |  |
| ANALYZER_KEYWORD | 1 | Keyword analyzer is a “noop” analyzer which returns the entire input string as a single token. |
| ANALYZER_STANDARD | 2 | Standard analyzer provides grammar based tokenization |
| ANALYZER_SIMPLE | 3 | Simple analyzer breaks text into tokens at any non-letter character, such as numbers, spaces, hyphens and apostrophes, discards non-letter characters, and changes uppercase to lowercase. |



<a name="banyandb-database-v1-IndexRule-Type"></a>

### IndexRule.Type
Type determine the index structure under the hood

| Name | Number | Description |
| ---- | ------ | ----------- |
| TYPE_UNSPECIFIED | 0 |  |
| TYPE_INVERTED | 1 |  |



<a name="banyandb-database-v1-TagType"></a>

### TagType


| Name | Number | Description |
| ---- | ------ | ----------- |
| TAG_TYPE_UNSPECIFIED | 0 |  |
| TAG_TYPE_STRING | 1 |  |
| TAG_TYPE_INT | 2 |  |
| TAG_TYPE_STRING_ARRAY | 3 |  |
| TAG_TYPE_INT_ARRAY | 4 |  |
| TAG_TYPE_DATA_BINARY | 5 |  |


 

 

 



<a name="banyandb_database_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/rpc.proto



<a name="banyandb-database-v1-GroupRegistryServiceCreateRequest"></a>

### GroupRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceCreateResponse"></a>

### GroupRegistryServiceCreateResponse







<a name="banyandb-database-v1-GroupRegistryServiceDeleteRequest"></a>

### GroupRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceDeleteResponse"></a>

### GroupRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceExistRequest"></a>

### GroupRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceExistResponse"></a>

### GroupRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceGetRequest"></a>

### GroupRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceGetResponse"></a>

### GroupRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceListRequest"></a>

### GroupRegistryServiceListRequest







<a name="banyandb-database-v1-GroupRegistryServiceListResponse"></a>

### GroupRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) | repeated |  |






<a name="banyandb-database-v1-GroupRegistryServiceUpdateRequest"></a>

### GroupRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [banyandb.common.v1.Group](#banyandb-common-v1-Group) |  |  |






<a name="banyandb-database-v1-GroupRegistryServiceUpdateResponse"></a>

### GroupRegistryServiceUpdateResponse







<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest"></a>

### IndexRuleBindingRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse"></a>

### IndexRuleBindingRegistryServiceCreateResponse







<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest"></a>

### IndexRuleBindingRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse"></a>

### IndexRuleBindingRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceExistRequest"></a>

### IndexRuleBindingRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceExistResponse"></a>

### IndexRuleBindingRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_index_rule_binding | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest"></a>

### IndexRuleBindingRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse"></a>

### IndexRuleBindingRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest"></a>

### IndexRuleBindingRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse"></a>

### IndexRuleBindingRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) | repeated |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest"></a>

### IndexRuleBindingRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule_binding | [IndexRuleBinding](#banyandb-database-v1-IndexRuleBinding) |  |  |






<a name="banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse"></a>

### IndexRuleBindingRegistryServiceUpdateResponse







<a name="banyandb-database-v1-IndexRuleRegistryServiceCreateRequest"></a>

### IndexRuleRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceCreateResponse"></a>

### IndexRuleRegistryServiceCreateResponse







<a name="banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest"></a>

### IndexRuleRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse"></a>

### IndexRuleRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceExistRequest"></a>

### IndexRuleRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceExistResponse"></a>

### IndexRuleRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_index_rule | [bool](#bool) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceGetRequest"></a>

### IndexRuleRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceGetResponse"></a>

### IndexRuleRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceListRequest"></a>

### IndexRuleRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceListResponse"></a>

### IndexRuleRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) | repeated |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest"></a>

### IndexRuleRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index_rule | [IndexRule](#banyandb-database-v1-IndexRule) |  |  |






<a name="banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse"></a>

### IndexRuleRegistryServiceUpdateResponse







<a name="banyandb-database-v1-MeasureRegistryServiceCreateRequest"></a>

### MeasureRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceCreateResponse"></a>

### MeasureRegistryServiceCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceDeleteRequest"></a>

### MeasureRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceDeleteResponse"></a>

### MeasureRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceExistRequest"></a>

### MeasureRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceExistResponse"></a>

### MeasureRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_measure | [bool](#bool) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceGetRequest"></a>

### MeasureRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceGetResponse"></a>

### MeasureRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceListRequest"></a>

### MeasureRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceListResponse"></a>

### MeasureRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) | repeated |  |






<a name="banyandb-database-v1-MeasureRegistryServiceUpdateRequest"></a>

### MeasureRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| measure | [Measure](#banyandb-database-v1-Measure) |  |  |






<a name="banyandb-database-v1-MeasureRegistryServiceUpdateResponse"></a>

### MeasureRegistryServiceUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceCreateRequest"></a>

### StreamRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceCreateResponse"></a>

### StreamRegistryServiceCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceDeleteRequest"></a>

### StreamRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceDeleteResponse"></a>

### StreamRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceExistRequest"></a>

### StreamRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceExistResponse"></a>

### StreamRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_stream | [bool](#bool) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceGetRequest"></a>

### StreamRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceGetResponse"></a>

### StreamRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceListRequest"></a>

### StreamRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceListResponse"></a>

### StreamRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) | repeated |  |






<a name="banyandb-database-v1-StreamRegistryServiceUpdateRequest"></a>

### StreamRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceUpdateResponse"></a>

### StreamRegistryServiceUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mod_revision | [int64](#int64) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest"></a>

### TopNAggregationRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse"></a>

### TopNAggregationRegistryServiceCreateResponse







<a name="banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest"></a>

### TopNAggregationRegistryServiceDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse"></a>

### TopNAggregationRegistryServiceDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceExistRequest"></a>

### TopNAggregationRegistryServiceExistRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceExistResponse"></a>

### TopNAggregationRegistryServiceExistResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_group | [bool](#bool) |  |  |
| has_top_n_aggregation | [bool](#bool) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceGetRequest"></a>

### TopNAggregationRegistryServiceGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceGetResponse"></a>

### TopNAggregationRegistryServiceGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceListRequest"></a>

### TopNAggregationRegistryServiceListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceListResponse"></a>

### TopNAggregationRegistryServiceListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) | repeated |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest"></a>

### TopNAggregationRegistryServiceUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_n_aggregation | [TopNAggregation](#banyandb-database-v1-TopNAggregation) |  |  |






<a name="banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse"></a>

### TopNAggregationRegistryServiceUpdateResponse






 

 

 


<a name="banyandb-database-v1-GroupRegistryService"></a>

### GroupRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [GroupRegistryServiceCreateRequest](#banyandb-database-v1-GroupRegistryServiceCreateRequest) | [GroupRegistryServiceCreateResponse](#banyandb-database-v1-GroupRegistryServiceCreateResponse) |  |
| Update | [GroupRegistryServiceUpdateRequest](#banyandb-database-v1-GroupRegistryServiceUpdateRequest) | [GroupRegistryServiceUpdateResponse](#banyandb-database-v1-GroupRegistryServiceUpdateResponse) |  |
| Delete | [GroupRegistryServiceDeleteRequest](#banyandb-database-v1-GroupRegistryServiceDeleteRequest) | [GroupRegistryServiceDeleteResponse](#banyandb-database-v1-GroupRegistryServiceDeleteResponse) |  |
| Get | [GroupRegistryServiceGetRequest](#banyandb-database-v1-GroupRegistryServiceGetRequest) | [GroupRegistryServiceGetResponse](#banyandb-database-v1-GroupRegistryServiceGetResponse) |  |
| List | [GroupRegistryServiceListRequest](#banyandb-database-v1-GroupRegistryServiceListRequest) | [GroupRegistryServiceListResponse](#banyandb-database-v1-GroupRegistryServiceListResponse) |  |
| Exist | [GroupRegistryServiceExistRequest](#banyandb-database-v1-GroupRegistryServiceExistRequest) | [GroupRegistryServiceExistResponse](#banyandb-database-v1-GroupRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-IndexRuleBindingRegistryService"></a>

### IndexRuleBindingRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [IndexRuleBindingRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest) | [IndexRuleBindingRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse) |  |
| Update | [IndexRuleBindingRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest) | [IndexRuleBindingRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse) |  |
| Delete | [IndexRuleBindingRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest) | [IndexRuleBindingRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse) |  |
| Get | [IndexRuleBindingRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest) | [IndexRuleBindingRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse) |  |
| List | [IndexRuleBindingRegistryServiceListRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest) | [IndexRuleBindingRegistryServiceListResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse) |  |
| Exist | [IndexRuleBindingRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistRequest) | [IndexRuleBindingRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-IndexRuleRegistryService"></a>

### IndexRuleRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [IndexRuleRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleRegistryServiceCreateRequest) | [IndexRuleRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleRegistryServiceCreateResponse) |  |
| Update | [IndexRuleRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest) | [IndexRuleRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse) |  |
| Delete | [IndexRuleRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest) | [IndexRuleRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse) |  |
| Get | [IndexRuleRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleRegistryServiceGetRequest) | [IndexRuleRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleRegistryServiceGetResponse) |  |
| List | [IndexRuleRegistryServiceListRequest](#banyandb-database-v1-IndexRuleRegistryServiceListRequest) | [IndexRuleRegistryServiceListResponse](#banyandb-database-v1-IndexRuleRegistryServiceListResponse) |  |
| Exist | [IndexRuleRegistryServiceExistRequest](#banyandb-database-v1-IndexRuleRegistryServiceExistRequest) | [IndexRuleRegistryServiceExistResponse](#banyandb-database-v1-IndexRuleRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-MeasureRegistryService"></a>

### MeasureRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [MeasureRegistryServiceCreateRequest](#banyandb-database-v1-MeasureRegistryServiceCreateRequest) | [MeasureRegistryServiceCreateResponse](#banyandb-database-v1-MeasureRegistryServiceCreateResponse) |  |
| Update | [MeasureRegistryServiceUpdateRequest](#banyandb-database-v1-MeasureRegistryServiceUpdateRequest) | [MeasureRegistryServiceUpdateResponse](#banyandb-database-v1-MeasureRegistryServiceUpdateResponse) |  |
| Delete | [MeasureRegistryServiceDeleteRequest](#banyandb-database-v1-MeasureRegistryServiceDeleteRequest) | [MeasureRegistryServiceDeleteResponse](#banyandb-database-v1-MeasureRegistryServiceDeleteResponse) |  |
| Get | [MeasureRegistryServiceGetRequest](#banyandb-database-v1-MeasureRegistryServiceGetRequest) | [MeasureRegistryServiceGetResponse](#banyandb-database-v1-MeasureRegistryServiceGetResponse) |  |
| List | [MeasureRegistryServiceListRequest](#banyandb-database-v1-MeasureRegistryServiceListRequest) | [MeasureRegistryServiceListResponse](#banyandb-database-v1-MeasureRegistryServiceListResponse) |  |
| Exist | [MeasureRegistryServiceExistRequest](#banyandb-database-v1-MeasureRegistryServiceExistRequest) | [MeasureRegistryServiceExistResponse](#banyandb-database-v1-MeasureRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-StreamRegistryService"></a>

### StreamRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [StreamRegistryServiceCreateRequest](#banyandb-database-v1-StreamRegistryServiceCreateRequest) | [StreamRegistryServiceCreateResponse](#banyandb-database-v1-StreamRegistryServiceCreateResponse) |  |
| Update | [StreamRegistryServiceUpdateRequest](#banyandb-database-v1-StreamRegistryServiceUpdateRequest) | [StreamRegistryServiceUpdateResponse](#banyandb-database-v1-StreamRegistryServiceUpdateResponse) |  |
| Delete | [StreamRegistryServiceDeleteRequest](#banyandb-database-v1-StreamRegistryServiceDeleteRequest) | [StreamRegistryServiceDeleteResponse](#banyandb-database-v1-StreamRegistryServiceDeleteResponse) |  |
| Get | [StreamRegistryServiceGetRequest](#banyandb-database-v1-StreamRegistryServiceGetRequest) | [StreamRegistryServiceGetResponse](#banyandb-database-v1-StreamRegistryServiceGetResponse) |  |
| List | [StreamRegistryServiceListRequest](#banyandb-database-v1-StreamRegistryServiceListRequest) | [StreamRegistryServiceListResponse](#banyandb-database-v1-StreamRegistryServiceListResponse) |  |
| Exist | [StreamRegistryServiceExistRequest](#banyandb-database-v1-StreamRegistryServiceExistRequest) | [StreamRegistryServiceExistResponse](#banyandb-database-v1-StreamRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |


<a name="banyandb-database-v1-TopNAggregationRegistryService"></a>

### TopNAggregationRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [TopNAggregationRegistryServiceCreateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest) | [TopNAggregationRegistryServiceCreateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse) |  |
| Update | [TopNAggregationRegistryServiceUpdateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest) | [TopNAggregationRegistryServiceUpdateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse) |  |
| Delete | [TopNAggregationRegistryServiceDeleteRequest](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest) | [TopNAggregationRegistryServiceDeleteResponse](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse) |  |
| Get | [TopNAggregationRegistryServiceGetRequest](#banyandb-database-v1-TopNAggregationRegistryServiceGetRequest) | [TopNAggregationRegistryServiceGetResponse](#banyandb-database-v1-TopNAggregationRegistryServiceGetResponse) |  |
| List | [TopNAggregationRegistryServiceListRequest](#banyandb-database-v1-TopNAggregationRegistryServiceListRequest) | [TopNAggregationRegistryServiceListResponse](#banyandb-database-v1-TopNAggregationRegistryServiceListResponse) |  |
| Exist | [TopNAggregationRegistryServiceExistRequest](#banyandb-database-v1-TopNAggregationRegistryServiceExistRequest) | [TopNAggregationRegistryServiceExistResponse](#banyandb-database-v1-TopNAggregationRegistryServiceExistResponse) | Exist doesn&#39;t expose an HTTP endpoint. Please use HEAD method to touch Get instead |

 



<a name="banyandb_measure_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/query.proto



<a name="banyandb-measure-v1-DataPoint"></a>

### DataPoint
DataPoint is stored in Measures


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. |
| tag_families | [banyandb.model.v1.TagFamily](#banyandb-model-v1-TagFamily) | repeated | tag_families contains tags selected in the projection |
| fields | [DataPoint.Field](#banyandb-measure-v1-DataPoint-Field) | repeated | fields contains fields selected in the projection |






<a name="banyandb-measure-v1-DataPoint-Field"></a>

### DataPoint.Field



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| value | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) |  |  |






<a name="banyandb-measure-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the data points are stored. |
| name | [string](#string) |  | name is the identity of a measure. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | tag_families are indexed. |
| tag_projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | tag_projection can be used to select tags of the data points in the response |
| field_projection | [QueryRequest.FieldProjection](#banyandb-measure-v1-QueryRequest-FieldProjection) |  | field_projection can be used to select fields of the data points in the response |
| group_by | [QueryRequest.GroupBy](#banyandb-measure-v1-QueryRequest-GroupBy) |  | group_by groups data points based on their field value for a specific tag and use field_name as the projection name |
| agg | [QueryRequest.Aggregation](#banyandb-measure-v1-QueryRequest-Aggregation) |  | agg aggregates data points based on a field |
| top | [QueryRequest.Top](#banyandb-measure-v1-QueryRequest-Top) |  | top limits the result based on a particular field. If order_by is specified, top sorts the dataset based on order_by&#39;s output |
| offset | [uint32](#uint32) |  | offset is used to support pagination, together with the following limit. If top is specified, offset processes the dataset based on top&#39;s output |
| limit | [uint32](#uint32) |  | limit is used to impose a boundary on the number of records being returned. If top is specified, limit processes the dataset based on top&#39;s output |
| order_by | [banyandb.model.v1.QueryOrder](#banyandb-model-v1-QueryOrder) |  | order_by is given to specify the sort for a tag. |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |






<a name="banyandb-measure-v1-QueryRequest-Aggregation"></a>

### QueryRequest.Aggregation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| function | [banyandb.model.v1.AggregationFunction](#banyandb-model-v1-AggregationFunction) |  |  |
| field_name | [string](#string) |  | field_name must be one of files indicated by the field_projection |






<a name="banyandb-measure-v1-QueryRequest-FieldProjection"></a>

### QueryRequest.FieldProjection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| names | [string](#string) | repeated |  |






<a name="banyandb-measure-v1-QueryRequest-GroupBy"></a>

### QueryRequest.GroupBy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | tag_projection must be a subset of the tag_projection of QueryRequest |
| field_name | [string](#string) |  | field_name must be one of fields indicated by field_projection |






<a name="banyandb-measure-v1-QueryRequest-Top"></a>

### QueryRequest.Top



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| number | [int32](#int32) |  | number set the how many items should be returned |
| field_name | [string](#string) |  | field_name must be one of files indicated by the field_projection |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields ASC: bottomN DESC: topN UNSPECIFIED: topN |






<a name="banyandb-measure-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data_points | [DataPoint](#banyandb-measure-v1-DataPoint) | repeated | data_points are the actual data returned |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |





 

 

 

 



<a name="banyandb_measure_v1_topn-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/topn.proto



<a name="banyandb-measure-v1-TopNList"></a>

### TopNList
TopNList contains a series of topN items


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. |
| items | [TopNList.Item](#banyandb-measure-v1-TopNList-Item) | repeated | items contains top-n items in a list |






<a name="banyandb-measure-v1-TopNList-Item"></a>

### TopNList.Item



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity | [banyandb.model.v1.Tag](#banyandb-model-v1-Tag) | repeated |  |
| value | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) |  |  |






<a name="banyandb-measure-v1-TopNRequest"></a>

### TopNRequest
TopNRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the data points are stored. |
| name | [string](#string) |  | name is the identity of a measure. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. |
| top_n | [int32](#int32) |  | top_n set the how many items should be returned in each list. |
| agg | [banyandb.model.v1.AggregationFunction](#banyandb-model-v1-AggregationFunction) |  | agg aggregates lists grouped by field names in the time_range TODO validate enum defined_only |
| conditions | [banyandb.model.v1.Condition](#banyandb-model-v1-Condition) | repeated | criteria select counters. Only equals are acceptable. |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields |






<a name="banyandb-measure-v1-TopNResponse"></a>

### TopNResponse
TopNResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lists | [TopNList](#banyandb-measure-v1-TopNList) | repeated | lists contain a series topN lists ranked by timestamp if agg_func in query request is specified, lists&#39; size should be one. |





 

 

 

 



<a name="banyandb_model_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/model/v1/write.proto


 


<a name="banyandb-model-v1-Status"></a>

### Status
Status is the response status for write

| Name | Number | Description |
| ---- | ------ | ----------- |
| STATUS_UNSPECIFIED | 0 |  |
| STATUS_SUCCEED | 1 |  |
| STATUS_INVALID_TIMESTAMP | 2 |  |
| STATUS_NOT_FOUND | 3 |  |
| STATUS_EXPIRED_SCHEMA | 4 |  |
| STATUS_INTERNAL_ERROR | 5 |  |


 

 

 



<a name="banyandb_measure_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/write.proto



<a name="banyandb-measure-v1-DataPointValue"></a>

### DataPointValue
DataPointValue is the data point for writing. It only contains values.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. |
| tag_families | [banyandb.model.v1.TagFamilyForWrite](#banyandb-model-v1-TagFamilyForWrite) | repeated | the order of tag_families&#39; items match the measure schema |
| fields | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) | repeated | the order of fields match the measure schema |






<a name="banyandb-measure-v1-InternalWriteRequest"></a>

### InternalWriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) |  |  |
| series_hash | [bytes](#bytes) |  |  |
| entity_values | [banyandb.model.v1.TagValue](#banyandb-model-v1-TagValue) | repeated |  |
| request | [WriteRequest](#banyandb-measure-v1-WriteRequest) |  |  |






<a name="banyandb-measure-v1-WriteRequest"></a>

### WriteRequest
WriteRequest is the request contract for write


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata is required. |
| data_point | [DataPointValue](#banyandb-measure-v1-DataPointValue) |  | the data_point is required. |
| message_id | [uint64](#uint64) |  | the message_id is required. |






<a name="banyandb-measure-v1-WriteResponse"></a>

### WriteResponse
WriteResponse is the response contract for write


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message_id | [uint64](#uint64) |  | the message_id from request. |
| status | [banyandb.model.v1.Status](#banyandb-model-v1-Status) |  | status indicates the request processing result |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata from request when request fails |





 

 

 

 



<a name="banyandb_measure_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/measure/v1/rpc.proto


 

 

 


<a name="banyandb-measure-v1-MeasureService"></a>

### MeasureService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Query | [QueryRequest](#banyandb-measure-v1-QueryRequest) | [QueryResponse](#banyandb-measure-v1-QueryResponse) |  |
| Write | [WriteRequest](#banyandb-measure-v1-WriteRequest) stream | [WriteResponse](#banyandb-measure-v1-WriteResponse) stream |  |
| TopN | [TopNRequest](#banyandb-measure-v1-TopNRequest) | [TopNResponse](#banyandb-measure-v1-TopNResponse) |  |

 



<a name="banyandb_property_v1_property-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/property.proto



<a name="banyandb-property-v1-Metadata"></a>

### Metadata
Metadata is for multi-tenant use


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | container is created when it receives the first property |
| id | [string](#string) |  | id identifies a property |






<a name="banyandb-property-v1-Property"></a>

### Property
Property stores the user defined data


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-property-v1-Metadata) |  | metadata is the identity of a property |
| tags | [banyandb.model.v1.Tag](#banyandb-model-v1-Tag) | repeated | tag stores the content of a property |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the property is updated |
| lease_id | [int64](#int64) |  | readonly. lease_id is the ID of the lease that attached to key. |
| ttl | [string](#string) |  | ttl indicates the time to live of the property. It&#39;s a string in the format of &#34;1h&#34;, &#34;2m&#34;, &#34;3s&#34;, &#34;1500ms&#34;. It defaults to 0s, which means the property never expires. The minimum allowed ttl is 1s. |





 

 

 

 



<a name="banyandb_property_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/rpc.proto



<a name="banyandb-property-v1-ApplyRequest"></a>

### ApplyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) |  |  |
| strategy | [ApplyRequest.Strategy](#banyandb-property-v1-ApplyRequest-Strategy) |  | strategy indicates how to update a property. It defaults to STRATEGY_MERGE |






<a name="banyandb-property-v1-ApplyResponse"></a>

### ApplyResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| created | [bool](#bool) |  | created indicates whether the property existed. True: the property is absent. False: the property existed. |
| tags_num | [uint32](#uint32) |  |  |
| lease_id | [int64](#int64) |  |  |






<a name="banyandb-property-v1-DeleteRequest"></a>

### DeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-property-v1-Metadata) |  |  |
| tags | [string](#string) | repeated |  |






<a name="banyandb-property-v1-DeleteResponse"></a>

### DeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |
| tags_num | [uint32](#uint32) |  |  |






<a name="banyandb-property-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-property-v1-Metadata) |  |  |
| tags | [string](#string) | repeated |  |






<a name="banyandb-property-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) |  |  |






<a name="banyandb-property-v1-KeepAliveRequest"></a>

### KeepAliveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lease_id | [int64](#int64) |  |  |






<a name="banyandb-property-v1-KeepAliveResponse"></a>

### KeepAliveResponse







<a name="banyandb-property-v1-ListRequest"></a>

### ListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| ids | [string](#string) | repeated |  |
| tags | [string](#string) | repeated |  |






<a name="banyandb-property-v1-ListResponse"></a>

### ListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) | repeated |  |





 


<a name="banyandb-property-v1-ApplyRequest-Strategy"></a>

### ApplyRequest.Strategy


| Name | Number | Description |
| ---- | ------ | ----------- |
| STRATEGY_UNSPECIFIED | 0 |  |
| STRATEGY_MERGE | 1 |  |
| STRATEGY_REPLACE | 2 |  |


 

 


<a name="banyandb-property-v1-PropertyService"></a>

### PropertyService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Apply | [ApplyRequest](#banyandb-property-v1-ApplyRequest) | [ApplyResponse](#banyandb-property-v1-ApplyResponse) | Apply creates a property if it&#39;s absent, or update a existed one based on a strategy. |
| Delete | [DeleteRequest](#banyandb-property-v1-DeleteRequest) | [DeleteResponse](#banyandb-property-v1-DeleteResponse) |  |
| Get | [GetRequest](#banyandb-property-v1-GetRequest) | [GetResponse](#banyandb-property-v1-GetResponse) |  |
| List | [ListRequest](#banyandb-property-v1-ListRequest) | [ListResponse](#banyandb-property-v1-ListResponse) |  |
| KeepAlive | [KeepAliveRequest](#banyandb-property-v1-KeepAliveRequest) | [KeepAliveResponse](#banyandb-property-v1-KeepAliveResponse) |  |

 



<a name="banyandb_stream_v1_query-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/stream/v1/query.proto



<a name="banyandb-stream-v1-Element"></a>

### Element
Element represents
(stream context) a Span defined in Google Dapper paper or equivalently a Segment in Skywalking.
(Log context) a log


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element_id | [string](#string) |  | element_id could be span_id of a Span or segment_id of a Segment in the context of stream |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp represents a millisecond 1) either the start time of a Span/Segment, 2) or the timestamp of a log |
| tag_families | [banyandb.model.v1.TagFamily](#banyandb-model-v1-TagFamily) | repeated | fields contains all indexed Field. Some typical names, - stream_id - duration - service_name - service_instance_id - end_time_milliseconds |






<a name="banyandb-stream-v1-QueryRequest"></a>

### QueryRequest
QueryRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| groups | [string](#string) | repeated | groups indicate where the elements are stored. |
| name | [string](#string) |  | name is the identity of a stream. |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. In the context of stream, it represents the range of the `startTime` for spans/segments, while in the context of Log, it means the range of the timestamp(s) for logs. it is always recommended to specify time range for performance reason |
| offset | [uint32](#uint32) |  | offset is used to support pagination, together with the following limit |
| limit | [uint32](#uint32) |  | limit is used to impose a boundary on the number of records being returned |
| order_by | [banyandb.model.v1.QueryOrder](#banyandb-model-v1-QueryOrder) |  | order_by is given to specify the sort for a field. So far, only fields in the type of Integer are supported |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) |  | tag_families are indexed. |
| projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | projection can be used to select the key names of the element in the response |
| trace | [bool](#bool) |  | trace is used to enable trace for the query |






<a name="banyandb-stream-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| elements | [Element](#banyandb-stream-v1-Element) | repeated | elements are the actual data returned |
| trace | [banyandb.common.v1.Trace](#banyandb-common-v1-Trace) |  | trace contains the trace information of the query when trace is enabled |





 

 

 

 



<a name="banyandb_stream_v1_write-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/stream/v1/write.proto



<a name="banyandb-stream-v1-ElementValue"></a>

### ElementValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element_id | [string](#string) |  | element_id could be span_id of a Span or segment_id of a Segment in the context of stream |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | timestamp is in the timeunit of milliseconds. It represents 1) either the start time of a Span/Segment, 2) or the timestamp of a log |
| tag_families | [banyandb.model.v1.TagFamilyForWrite](#banyandb-model-v1-TagFamilyForWrite) | repeated | the order of tag_families&#39; items match the stream schema |






<a name="banyandb-stream-v1-InternalWriteRequest"></a>

### InternalWriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_id | [uint32](#uint32) |  |  |
| series_hash | [bytes](#bytes) |  |  |
| entity_values | [banyandb.model.v1.TagValue](#banyandb-model-v1-TagValue) | repeated |  |
| request | [WriteRequest](#banyandb-stream-v1-WriteRequest) |  |  |






<a name="banyandb-stream-v1-WriteRequest"></a>

### WriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata is required. |
| element | [ElementValue](#banyandb-stream-v1-ElementValue) |  | the element is required. |
| message_id | [uint64](#uint64) |  | the message_id is required. |






<a name="banyandb-stream-v1-WriteResponse"></a>

### WriteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message_id | [uint64](#uint64) |  | the message_id from request. |
| status | [banyandb.model.v1.Status](#banyandb-model-v1-Status) |  | status indicates the request processing result |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata from request when request fails |





 

 

 

 



<a name="banyandb_stream_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/stream/v1/rpc.proto


 

 

 


<a name="banyandb-stream-v1-StreamService"></a>

### StreamService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Query | [QueryRequest](#banyandb-stream-v1-QueryRequest) | [QueryResponse](#banyandb-stream-v1-QueryResponse) |  |
| Write | [WriteRequest](#banyandb-stream-v1-WriteRequest) stream | [WriteResponse](#banyandb-stream-v1-WriteResponse) stream |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

