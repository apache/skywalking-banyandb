# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/measure/v1/query.proto](#banyandb_measure_v1_query-proto)
    - [DataPoint](#banyandb-measure-v1-DataPoint)
    - [DataPoint.Field](#banyandb-measure-v1-DataPoint-Field)
    - [QueryRequest](#banyandb-measure-v1-QueryRequest)
    - [QueryRequest.Aggregation](#banyandb-measure-v1-QueryRequest-Aggregation)
    - [QueryRequest.FieldProjection](#banyandb-measure-v1-QueryRequest-FieldProjection)
    - [QueryRequest.GroupBy](#banyandb-measure-v1-QueryRequest-GroupBy)
    - [QueryRequest.Top](#banyandb-measure-v1-QueryRequest-Top)
    - [QueryResponse](#banyandb-measure-v1-QueryResponse)
  
- [banyandb/measure/v1/write.proto](#banyandb_measure_v1_write-proto)
    - [DataPointValue](#banyandb-measure-v1-DataPointValue)
    - [InternalWriteRequest](#banyandb-measure-v1-InternalWriteRequest)
    - [WriteRequest](#banyandb-measure-v1-WriteRequest)
    - [WriteResponse](#banyandb-measure-v1-WriteResponse)
  
- [banyandb/measure/v1/topn.proto](#banyandb_measure_v1_topn-proto)
    - [TopNList](#banyandb-measure-v1-TopNList)
    - [TopNList.Item](#banyandb-measure-v1-TopNList-Item)
    - [TopNRequest](#banyandb-measure-v1-TopNRequest)
    - [TopNResponse](#banyandb-measure-v1-TopNResponse)
  
- [banyandb/measure/v1/rpc.proto](#banyandb_measure_v1_rpc-proto)
    - [MeasureService](#banyandb-measure-v1-MeasureService)
  
- [Scalar Value Types](#scalar-value-types)



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
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is required |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) | repeated | tag_families are indexed. |
| tag_projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | tag_projection can be used to select tags of the data points in the response |
| field_projection | [QueryRequest.FieldProjection](#banyandb-measure-v1-QueryRequest-FieldProjection) |  | field_projection can be used to select fields of the data points in the response |
| group_by | [QueryRequest.GroupBy](#banyandb-measure-v1-QueryRequest-GroupBy) |  | group_by groups data points based on their field value for a specific tag and use field_name as the projection name |
| agg | [QueryRequest.Aggregation](#banyandb-measure-v1-QueryRequest-Aggregation) |  | agg aggregates data points based on a field |
| top | [QueryRequest.Top](#banyandb-measure-v1-QueryRequest-Top) |  | top limit the result based on a particular field |






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
| request | [WriteRequest](#banyandb-measure-v1-WriteRequest) |  |  |






<a name="banyandb-measure-v1-WriteRequest"></a>

### WriteRequest
WriteRequest is the request contract for write


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata is required. |
| data_point | [DataPointValue](#banyandb-measure-v1-DataPointValue) |  | the data_point is required. |






<a name="banyandb-measure-v1-WriteResponse"></a>

### WriteResponse
WriteResponse is the response contract for write





 

 

 

 



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
| name | [string](#string) |  |  |
| value | [banyandb.model.v1.FieldValue](#banyandb-model-v1-FieldValue) |  |  |






<a name="banyandb-measure-v1-TopNRequest"></a>

### TopNRequest
TopNRequest is the request contract for query.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is required |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. |
| top_n | [int32](#int32) |  | top_n set the how many items should be returned in each list. |
| agg | [banyandb.model.v1.AggregationFunction](#banyandb-model-v1-AggregationFunction) |  | agg aggregates lists grouped by field names in the time_range |
| conditions | [banyandb.model.v1.Condition](#banyandb-model-v1-Condition) | repeated | criteria select counters. |
| field_value_sort | [banyandb.model.v1.Sort](#banyandb-model-v1-Sort) |  | field_value_sort indicates how to sort fields |






<a name="banyandb-measure-v1-TopNResponse"></a>

### TopNResponse
TopNResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lists | [TopNList](#banyandb-measure-v1-TopNList) | repeated | lists contain a series topN lists ranked by timestamp if agg_func in query request is specified, lists&#39; size should be one. |





 

 

 

 



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

