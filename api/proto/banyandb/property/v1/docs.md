# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/property/v1/property.proto](#banyandb_property_v1_property-proto)
    - [Metadata](#banyandb-property-v1-Metadata)
    - [Property](#banyandb-property-v1-Property)
  
- [banyandb/property/v1/rpc.proto](#banyandb_property_v1_rpc-proto)
    - [CreateRequest](#banyandb-property-v1-CreateRequest)
    - [CreateResponse](#banyandb-property-v1-CreateResponse)
    - [DeleteRequest](#banyandb-property-v1-DeleteRequest)
    - [DeleteResponse](#banyandb-property-v1-DeleteResponse)
    - [GetRequest](#banyandb-property-v1-GetRequest)
    - [GetResponse](#banyandb-property-v1-GetResponse)
    - [ListRequest](#banyandb-property-v1-ListRequest)
    - [ListResponse](#banyandb-property-v1-ListResponse)
    - [UpdateRequest](#banyandb-property-v1-UpdateRequest)
    - [UpdateResponse](#banyandb-property-v1-UpdateResponse)
  
    - [PropertyService](#banyandb-property-v1-PropertyService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="banyandb_property_v1_property-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/property.proto



<a name="banyandb-property-v1-Metadata"></a>

### Metadata
Metadata is for multi-tenant use


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  | container is created when it recevies the first property |
| id | [string](#string) |  | id identifies a property |






<a name="banyandb-property-v1-Property"></a>

### Property
Property stores the user defined data


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-property-v1-Metadata) |  | metadata is the identity of a property |
| tags | [banyandb.model.v1.Tag](#banyandb-model-v1-Tag) | repeated | tag stores the content of a property |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | updated_at indicates when the property is updated |





 

 

 

 



<a name="banyandb_property_v1_rpc-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## banyandb/property/v1/rpc.proto



<a name="banyandb-property-v1-CreateRequest"></a>

### CreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) |  |  |






<a name="banyandb-property-v1-CreateResponse"></a>

### CreateResponse







<a name="banyandb-property-v1-DeleteRequest"></a>

### DeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-property-v1-Metadata) |  |  |






<a name="banyandb-property-v1-DeleteResponse"></a>

### DeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="banyandb-property-v1-GetRequest"></a>

### GetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metadata | [Metadata](#banyandb-property-v1-Metadata) |  |  |






<a name="banyandb-property-v1-GetResponse"></a>

### GetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) |  |  |






<a name="banyandb-property-v1-ListRequest"></a>

### ListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| contaner | [banyandb.common.v1.Metadata](#banyandb-common-v1-Metadata) |  |  |






<a name="banyandb-property-v1-ListResponse"></a>

### ListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) | repeated |  |






<a name="banyandb-property-v1-UpdateRequest"></a>

### UpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| property | [Property](#banyandb-property-v1-Property) |  |  |






<a name="banyandb-property-v1-UpdateResponse"></a>

### UpdateResponse






 

 

 


<a name="banyandb-property-v1-PropertyService"></a>

### PropertyService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#banyandb-property-v1-CreateRequest) | [CreateResponse](#banyandb-property-v1-CreateResponse) |  |
| Update | [UpdateRequest](#banyandb-property-v1-UpdateRequest) | [UpdateResponse](#banyandb-property-v1-UpdateResponse) |  |
| Delete | [DeleteRequest](#banyandb-property-v1-DeleteRequest) | [DeleteResponse](#banyandb-property-v1-DeleteResponse) |  |
| Get | [GetRequest](#banyandb-property-v1-GetRequest) | [GetResponse](#banyandb-property-v1-GetResponse) |  |
| List | [ListRequest](#banyandb-property-v1-ListRequest) | [ListResponse](#banyandb-property-v1-ListResponse) |  |

 



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

