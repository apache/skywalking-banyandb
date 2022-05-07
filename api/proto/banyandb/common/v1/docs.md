# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [banyandb/common/v1/common.proto](#banyandb_common_v1_common-proto)
    - [Group](#banyandb-common-v1-Group)
    - [Metadata](#banyandb-common-v1-Metadata)
    - [ResourceOpts](#banyandb-common-v1-ResourceOpts)
  
    - [Catalog](#banyandb-common-v1-Catalog)
  
- [Scalar Value Types](#scalar-value-types)



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






<a name="banyandb-common-v1-Metadata"></a>

### Metadata
Metadata is for multi-tenant, multi-model use


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| group | [string](#string) |  | group contains a set of options, like retention policy, max |
| name | [string](#string) |  | name of the entity |
| id | [uint32](#uint32) |  |  |
| create_revision | [int64](#int64) |  | readonly. create_revision is the revision of last creation on this key. |
| mod_revision | [int64](#int64) |  | readonly. mod_revision is the revision of last modification on this key. |






<a name="banyandb-common-v1-ResourceOpts"></a>

### ResourceOpts



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shard_num | [uint32](#uint32) |  | shard_num is the number of shards |
| block_num | [uint32](#uint32) |  | block_num specific how many blocks in a segment |
| ttl | [string](#string) |  | ttl indicates time to live, how long the data will be cached |





 


<a name="banyandb-common-v1-Catalog"></a>

### Catalog


| Name | Number | Description |
| ---- | ------ | ----------- |
| CATALOG_UNSPECIFIED | 0 |  |
| CATALOG_STREAM | 1 |  |
| CATALOG_MEASURE | 2 |  |


 

 

 



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

