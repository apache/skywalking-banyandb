# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/database/v1/database.proto](#banyandb_database_v1_database-proto)
    - [Node](#banyandb-database-v1-Node)
    - [Shard](#banyandb-database-v1-Shard)
  
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
    - [IndexRule.Location](#banyandb-database-v1-IndexRule-Location)
    - [IndexRule.Type](#banyandb-database-v1-IndexRule-Type)
    - [TagType](#banyandb-database-v1-TagType)
  
- [banyandb/database/v1/event.proto](#banyandb_database_v1_event-proto)
    - [EntityEvent](#banyandb-database-v1-EntityEvent)
    - [EntityEvent.TagLocator](#banyandb-database-v1-EntityEvent-TagLocator)
    - [ShardEvent](#banyandb-database-v1-ShardEvent)
  
    - [Action](#banyandb-database-v1-Action)
  
- [banyandb/database/v1/rpc.proto](#banyandb_database_v1_rpc-proto)
    - [GroupRegistryServiceCreateRequest](#banyandb-database-v1-GroupRegistryServiceCreateRequest)
    - [GroupRegistryServiceCreateResponse](#banyandb-database-v1-GroupRegistryServiceCreateResponse)
    - [GroupRegistryServiceDeleteRequest](#banyandb-database-v1-GroupRegistryServiceDeleteRequest)
    - [GroupRegistryServiceDeleteResponse](#banyandb-database-v1-GroupRegistryServiceDeleteResponse)
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
  
- [Scalar Value Types](#scalar-value-types)



<a name="banyandb_database_v1_database-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/database.proto



<a name="banyandb-database-v1-Node"></a>

### Node



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| addr | [string](#string) |  |  |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="banyandb-database-v1-Shard"></a>

### Shard



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  |  |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  |  |
| node | [Node](#banyandb-database-v1-Node) |  |  |
| total | [uint32](#uint32) |  |  |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 

 

 

 



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
| location | [IndexRule.Location](#banyandb-database-v1-IndexRule-Location) |  | location indicates where to store index. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the IndexRule is updated |






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
| catalog | [banyandb.common.v1.Catalog](#banyandb-common-v1-Catalog) |  | catalog is where the subject belongs to |
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






<a name="banyandb-database-v1-TopNAggregation"></a>

### TopNAggregation
TopNAggregation generates offline TopN statistics for a measure&#39;s TopN approximation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is the identity of an aggregation |
| source_measure | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | source_measure denotes the data source of this aggregation |
| field_name | [string](#string) |  | field_name is the name of field used for ranking |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields ASC: bottomN DESC: topN UNSPECIFIED: topN &#43; bottomN |
| group_by_tag_names | [string](#string) | repeated | group_by_tag_names groups data points into statistical counters |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) | repeated | criteria select partial data points from measure |
| counters_number | [int32](#int32) |  | counters_number sets the number of counters to be tracked. The default value is 1000 |
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



<a name="banyandb-database-v1-IndexRule-Location"></a>

### IndexRule.Location


| Name | Number | Description |
| ---- | ------ | ----------- |
| LOCATION_UNSPECIFIED | 0 |  |
| LOCATION_SERIES | 1 |  |
| LOCATION_GLOBAL | 2 |  |



<a name="banyandb-database-v1-IndexRule-Type"></a>

### IndexRule.Type
Type determine the index structure under the hood

| Name | Number | Description |
| ---- | ------ | ----------- |
| TYPE_UNSPECIFIED | 0 |  |
| TYPE_TREE | 1 |  |
| TYPE_INVERTED | 2 |  |



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
| TAG_TYPE_ID | 6 |  |


 

 

 



<a name="banyandb_database_v1_event-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/database/v1/event.proto



<a name="banyandb-database-v1-EntityEvent"></a>

### EntityEvent



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| subject | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |
| entity_locator | [EntityEvent.TagLocator](#banyandb-database-v1-EntityEvent-TagLocator) | repeated |  |
| action | [Action](#banyandb-database-v1-Action) |  |  |
| time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |






<a name="banyandb-database-v1-EntityEvent-TagLocator"></a>

### EntityEvent.TagLocator



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| family_offset | [uint32](#uint32) |  |  |
| tag_offset | [uint32](#uint32) |  |  |






<a name="banyandb-database-v1-ShardEvent"></a>

### ShardEvent



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard | [Shard](#banyandb-database-v1-Shard) |  |  |
| action | [Action](#banyandb-database-v1-Action) |  |  |
| time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |





 


<a name="banyandb-database-v1-Action"></a>

### Action


| Name | Number | Description |
| ---- | ------ | ----------- |
| ACTION_UNSPECIFIED | 0 |  |
| ACTION_PUT | 1 |  |
| ACTION_DELETE | 2 |  |


 

 

 



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







<a name="banyandb-database-v1-StreamRegistryServiceCreateRequest"></a>

### StreamRegistryServiceCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stream | [Stream](#banyandb-database-v1-Stream) |  |  |






<a name="banyandb-database-v1-StreamRegistryServiceCreateResponse"></a>

### StreamRegistryServiceCreateResponse







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


<a name="banyandb-database-v1-IndexRuleBindingRegistryService"></a>

### IndexRuleBindingRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [IndexRuleBindingRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateRequest) | [IndexRuleBindingRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceCreateResponse) |  |
| Update | [IndexRuleBindingRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateRequest) | [IndexRuleBindingRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceUpdateResponse) |  |
| Delete | [IndexRuleBindingRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteRequest) | [IndexRuleBindingRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceDeleteResponse) |  |
| Get | [IndexRuleBindingRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetRequest) | [IndexRuleBindingRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceGetResponse) |  |
| List | [IndexRuleBindingRegistryServiceListRequest](#banyandb-database-v1-IndexRuleBindingRegistryServiceListRequest) | [IndexRuleBindingRegistryServiceListResponse](#banyandb-database-v1-IndexRuleBindingRegistryServiceListResponse) |  |


<a name="banyandb-database-v1-IndexRuleRegistryService"></a>

### IndexRuleRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [IndexRuleRegistryServiceCreateRequest](#banyandb-database-v1-IndexRuleRegistryServiceCreateRequest) | [IndexRuleRegistryServiceCreateResponse](#banyandb-database-v1-IndexRuleRegistryServiceCreateResponse) |  |
| Update | [IndexRuleRegistryServiceUpdateRequest](#banyandb-database-v1-IndexRuleRegistryServiceUpdateRequest) | [IndexRuleRegistryServiceUpdateResponse](#banyandb-database-v1-IndexRuleRegistryServiceUpdateResponse) |  |
| Delete | [IndexRuleRegistryServiceDeleteRequest](#banyandb-database-v1-IndexRuleRegistryServiceDeleteRequest) | [IndexRuleRegistryServiceDeleteResponse](#banyandb-database-v1-IndexRuleRegistryServiceDeleteResponse) |  |
| Get | [IndexRuleRegistryServiceGetRequest](#banyandb-database-v1-IndexRuleRegistryServiceGetRequest) | [IndexRuleRegistryServiceGetResponse](#banyandb-database-v1-IndexRuleRegistryServiceGetResponse) |  |
| List | [IndexRuleRegistryServiceListRequest](#banyandb-database-v1-IndexRuleRegistryServiceListRequest) | [IndexRuleRegistryServiceListResponse](#banyandb-database-v1-IndexRuleRegistryServiceListResponse) |  |


<a name="banyandb-database-v1-MeasureRegistryService"></a>

### MeasureRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [MeasureRegistryServiceCreateRequest](#banyandb-database-v1-MeasureRegistryServiceCreateRequest) | [MeasureRegistryServiceCreateResponse](#banyandb-database-v1-MeasureRegistryServiceCreateResponse) |  |
| Update | [MeasureRegistryServiceUpdateRequest](#banyandb-database-v1-MeasureRegistryServiceUpdateRequest) | [MeasureRegistryServiceUpdateResponse](#banyandb-database-v1-MeasureRegistryServiceUpdateResponse) |  |
| Delete | [MeasureRegistryServiceDeleteRequest](#banyandb-database-v1-MeasureRegistryServiceDeleteRequest) | [MeasureRegistryServiceDeleteResponse](#banyandb-database-v1-MeasureRegistryServiceDeleteResponse) |  |
| Get | [MeasureRegistryServiceGetRequest](#banyandb-database-v1-MeasureRegistryServiceGetRequest) | [MeasureRegistryServiceGetResponse](#banyandb-database-v1-MeasureRegistryServiceGetResponse) |  |
| List | [MeasureRegistryServiceListRequest](#banyandb-database-v1-MeasureRegistryServiceListRequest) | [MeasureRegistryServiceListResponse](#banyandb-database-v1-MeasureRegistryServiceListResponse) |  |


<a name="banyandb-database-v1-StreamRegistryService"></a>

### StreamRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [StreamRegistryServiceCreateRequest](#banyandb-database-v1-StreamRegistryServiceCreateRequest) | [StreamRegistryServiceCreateResponse](#banyandb-database-v1-StreamRegistryServiceCreateResponse) |  |
| Update | [StreamRegistryServiceUpdateRequest](#banyandb-database-v1-StreamRegistryServiceUpdateRequest) | [StreamRegistryServiceUpdateResponse](#banyandb-database-v1-StreamRegistryServiceUpdateResponse) |  |
| Delete | [StreamRegistryServiceDeleteRequest](#banyandb-database-v1-StreamRegistryServiceDeleteRequest) | [StreamRegistryServiceDeleteResponse](#banyandb-database-v1-StreamRegistryServiceDeleteResponse) |  |
| Get | [StreamRegistryServiceGetRequest](#banyandb-database-v1-StreamRegistryServiceGetRequest) | [StreamRegistryServiceGetResponse](#banyandb-database-v1-StreamRegistryServiceGetResponse) |  |
| List | [StreamRegistryServiceListRequest](#banyandb-database-v1-StreamRegistryServiceListRequest) | [StreamRegistryServiceListResponse](#banyandb-database-v1-StreamRegistryServiceListResponse) |  |


<a name="banyandb-database-v1-TopNAggregationRegistryService"></a>

### TopNAggregationRegistryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [TopNAggregationRegistryServiceCreateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceCreateRequest) | [TopNAggregationRegistryServiceCreateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceCreateResponse) |  |
| Update | [TopNAggregationRegistryServiceUpdateRequest](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateRequest) | [TopNAggregationRegistryServiceUpdateResponse](#banyandb-database-v1-TopNAggregationRegistryServiceUpdateResponse) |  |
| Delete | [TopNAggregationRegistryServiceDeleteRequest](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteRequest) | [TopNAggregationRegistryServiceDeleteResponse](#banyandb-database-v1-TopNAggregationRegistryServiceDeleteResponse) |  |
| Get | [TopNAggregationRegistryServiceGetRequest](#banyandb-database-v1-TopNAggregationRegistryServiceGetRequest) | [TopNAggregationRegistryServiceGetResponse](#banyandb-database-v1-TopNAggregationRegistryServiceGetResponse) |  |
| List | [TopNAggregationRegistryServiceListRequest](#banyandb-database-v1-TopNAggregationRegistryServiceListRequest) | [TopNAggregationRegistryServiceListResponse](#banyandb-database-v1-TopNAggregationRegistryServiceListResponse) |  |

 



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

