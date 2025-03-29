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

package v1

import (
	"bytes"
	"strings"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// ShardingKey determines the distribution of topN-related data.
// It is defined in the Measure schema.
type ShardingKey []Entry

// Marshal encodes a ShardingKey to bytes.
func (s ShardingKey) Marshal() []byte {
	data := make([][]byte, len(s))
	for i, entry := range s {
		data[i] = entry
	}
	return bytes.Join(data, nil)
}

// ShardingKeyValue represents the value of a tag which is a part of a ShardingKey.
type ShardingKeyValue = *modelv1.TagValue

// ShardingKeyValues is the encoded ShardingKey.
type ShardingKeyValues []ShardingKeyValue

// Encode ShardingKeyValues to tag values.
func (svs ShardingKeyValues) Encode() (result []*modelv1.TagValue) {
	for _, sv := range svs {
		result = append(result, sv)
	}
	return
}

// ToShardingKey transforms ShardingKeyValues to ShardingKey.
func (svs ShardingKeyValues) ToShardingKey() (result ShardingKey, err error) {
	for _, v := range svs {
		entry, errMarshal := MarshalTagValue(v)
		if errMarshal != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return
}

// String outputs the string represent of an EntityValue.
func (svs ShardingKeyValues) String() string {
	var strBuilder strings.Builder
	vv := svs.Encode()
	for i := 0; i < len(vv); i++ {
		strBuilder.WriteString(vv[i].String())
		if i < len(vv)-1 {
			strBuilder.WriteString(".")
		}
	}
	return strBuilder.String()
}

// ShardingKeyStrValue returns a ShardingKeyValue which wraps a string value.
func ShardingKeyStrValue(v string) ShardingKeyValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

// HashShardingKey runs hash function (e.g. with xxhash algorithm) on each segment of the ShardingKey,
// and concatenates all uint64 in byte array. So the return length of the byte array will be
// 8 (every uint64 has 8 bytes) * length of the input.
func HashShardingKey(shardingKey ShardingKey) []byte {
	result := make([]byte, 0, len(shardingKey)*8)
	for _, entry := range shardingKey {
		result = append(result, hash(entry)...)
	}
	return result
}
