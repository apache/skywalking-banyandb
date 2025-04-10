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
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// Entry is an element in an Entity.
type Entry []byte

// Entity denotes an identity of a Series.
// It is defined in the Stream or Measure schema.
type Entity []Entry

// Marshal encodes an Entity to bytes.
func (e Entity) Marshal() []byte {
	data := make([][]byte, len(e))
	for i, entry := range e {
		data[i] = entry
	}
	return bytes.Join(data, nil)
}

// EntityValue represents the value of a tag which is a part of an entity.
type EntityValue = *modelv1.TagValue

// EntityValues is the encoded Entity.
type EntityValues []EntityValue

// Encode EntityValues to tag values.
func (evs EntityValues) Encode() (result []*modelv1.TagValue) {
	for _, v := range evs {
		result = append(result, v)
	}
	return
}

// ToEntity transforms EntityValues to Entity.
func (evs EntityValues) ToEntity() (result Entity, err error) {
	for _, v := range evs {
		entry, errMarshal := MarshalTagValue(v)
		if errMarshal != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return
}

// String outputs the string represent of an EntityValue.
func (evs EntityValues) String() string {
	var strBuilder strings.Builder
	vv := evs.Encode()
	for i := 0; i < len(vv); i++ {
		strBuilder.WriteString(vv[i].String())
		if i < len(vv)-1 {
			strBuilder.WriteString(".")
		}
	}
	return strBuilder.String()
}

// EntityStrValue returns an EntityValue which wraps a string value.
func EntityStrValue(v string) EntityValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

// HashEntity runs hash function (e.g. with xxhash algorithm) on each segment of the Entity,
// and concatenates all uint64 in byte array. So the return length of the byte array will be
// 8 (every uint64 has 8 bytes) * length of the input.
func HashEntity(entity Entity) []byte {
	result := make([]byte, 0, len(entity)*8)
	for _, entry := range entity {
		result = append(result, hash(entry)...)
	}
	return result
}

func hash(entry []byte) []byte {
	return convert.Uint64ToBytes(convert.Hash(entry))
}
