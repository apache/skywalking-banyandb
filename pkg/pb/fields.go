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

package pb

import (
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

func Transform(entityValue *tracev1.EntityValue, fieldIndexes []FieldEntry) []*modelv1.TypedPair {
	typedPairs := make([]*modelv1.TypedPair, 0)
	if fieldIndexes != nil {
		// copy selected fields
		for _, fieldIndex := range fieldIndexes {
			key, idx := fieldIndex.Key, fieldIndex.Index
			if idx >= len(entityValue.GetFields()) {
				// skip
				continue
			}
			f := entityValue.GetFields()[idx]
			switch v := f.GetValueType().(type) {
			case *modelv1.Field_Str:
				typedPairs = append(typedPairs, buildPair(key, v.Str.GetValue()))
			case *modelv1.Field_StrArray:
				typedPairs = append(typedPairs, buildPair(key, v.StrArray.GetValue()))
			case *modelv1.Field_Int:
				typedPairs = append(typedPairs, buildPair(key, v.Int.GetValue()))
			case *modelv1.Field_IntArray:
				typedPairs = append(typedPairs, buildPair(key, v.IntArray.GetValue()))
			case *modelv1.Field_Null:
			}
		}
	} else {
		panic("what is the key?")
	}
	return typedPairs
}

type FieldEntry struct {
	Key   string
	Index int
}
