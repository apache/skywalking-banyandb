# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/model/v1/common.proto](#banyandb_model_v1_common-proto)
    - [FieldValue](#banyandb-model-v1-FieldValue)
    - [ID](#banyandb-model-v1-ID)
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
    - [QueryOrder](#banyandb-model-v1-QueryOrder)
    - [Tag](#banyandb-model-v1-Tag)
    - [TagFamily](#banyandb-model-v1-TagFamily)
    - [TagProjection](#banyandb-model-v1-TagProjection)
    - [TagProjection.TagFamily](#banyandb-model-v1-TagProjection-TagFamily)
    - [TimeRange](#banyandb-model-v1-TimeRange)
  
    - [Condition.BinaryOp](#banyandb-model-v1-Condition-BinaryOp)
    - [Sort](#banyandb-model-v1-Sort)
  
- [Scalar Value Types](#scalar-value-types)



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






<a name="banyandb-model-v1-ID"></a>

### ID



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






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
| id | [ID](#banyandb-model-v1-ID) |  |  |





 


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
| tag_family_name | [string](#string) |  |  |
| conditions | [Condition](#banyandb-model-v1-Condition) | repeated |  |






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



<a name="banyandb-model-v1-Sort"></a>

### Sort


| Name | Number | Description |
| ---- | ------ | ----------- |
| SORT_UNSPECIFIED | 0 |  |
| SORT_DESC | 1 |  |
| SORT_ASC | 2 |  |


 

 

 



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

