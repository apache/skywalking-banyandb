# Protocol Documentation
<a name="top"></a>

## Table of Contents

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
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | metadata is required |
| time_range | [banyandb.model.v1.TimeRange](#banyandb-model-v1-TimeRange) |  | time_range is a range query with begin/end time of entities in the timeunit of milliseconds. In the context of stream, it represents the range of the `startTime` for spans/segments, while in the context of Log, it means the range of the timestamp(s) for logs. it is always recommended to specify time range for performance reason |
| offset | [uint32](#uint32) |  | offset is used to support pagination, together with the following limit |
| limit | [uint32](#uint32) |  | limit is used to impose a boundary on the number of records being returned |
| order_by | [banyandb.model.v1.QueryOrder](#banyandb-model-v1-QueryOrder) |  | order_by is given to specify the sort for a field. So far, only fields in the type of Integer are supported |
| criteria | [banyandb.model.v1.Criteria](#banyandb-model-v1-Criteria) | repeated | tag_families are indexed. |
| projection | [banyandb.model.v1.TagProjection](#banyandb-model-v1-TagProjection) |  | projection can be used to select the key names of the element in the response |






<a name="banyandb-stream-v1-QueryResponse"></a>

### QueryResponse
QueryResponse is the response for a query to the Query module.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| elements | [Element](#banyandb-stream-v1-Element) | repeated | elements are the actual data returned |





 

 

 

 



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
| request | [WriteRequest](#banyandb-stream-v1-WriteRequest) |  |  |






<a name="banyandb-stream-v1-WriteRequest"></a>

### WriteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | the metadata is only required in the first write. |
| element | [ElementValue](#banyandb-stream-v1-ElementValue) |  | the element is required. |






<a name="banyandb-stream-v1-WriteResponse"></a>

### WriteResponse






 

 

 

 



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

