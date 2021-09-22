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

package v2

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func Transform(entityValue *tracev1.EntityValue, fieldIndexes []FieldEntry) []*modelv1.TypedPair {
	typedPairs := make([]*modelv1.TypedPair, 0)
	if fieldIndexes == nil {
		return typedPairs
	}
	// copy selected fields
	for _, fieldIndex := range fieldIndexes {
		key, idx, t := fieldIndex.Key, fieldIndex.Index, fieldIndex.Type
		if idx >= len(entityValue.GetFields()) {
			typedPairs = append(typedPairs, &modelv1.TypedPair{
				Key: key,
				Typed: &modelv1.TypedPair_NullPair{
					NullPair: &modelv1.TypedPair_NullWithType{Type: t},
				},
			})
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
			typedPairs = append(typedPairs, &modelv1.TypedPair{
				Key: key,
				Typed: &modelv1.TypedPair_NullPair{
					NullPair: &modelv1.TypedPair_NullWithType{Type: t},
				},
			})
		}
	}
	return typedPairs
}

func buildPair(key string, value interface{}) *modelv1.TypedPair {
	result := &modelv1.TypedPair{
		Key: key,
	}
	switch v := value.(type) {
	case int:
		result.Typed = &modelv1.TypedPair_IntPair{
			IntPair: &modelv1.Int{
				Value: int64(v),
			},
		}
	case []int:
		result.Typed = &modelv1.TypedPair_IntArrayPair{
			IntArrayPair: &modelv1.IntArray{
				Value: convert.IntToInt64(v...),
			},
		}
	case int8:
		result.Typed = &modelv1.TypedPair_IntPair{
			IntPair: &modelv1.Int{
				Value: int64(v),
			},
		}
	case []int8:
		result.Typed = &modelv1.TypedPair_IntArrayPair{
			IntArrayPair: &modelv1.IntArray{
				Value: convert.Int8ToInt64(v...),
			},
		}
	case int16:
		result.Typed = &modelv1.TypedPair_IntPair{
			IntPair: &modelv1.Int{
				Value: int64(v),
			},
		}
	case []int16:
		result.Typed = &modelv1.TypedPair_IntArrayPair{
			IntArrayPair: &modelv1.IntArray{
				Value: convert.Int16ToInt64(v...),
			},
		}
	case int32:
		result.Typed = &modelv1.TypedPair_IntPair{
			IntPair: &modelv1.Int{
				Value: int64(v),
			},
		}
	case []int32:
		result.Typed = &modelv1.TypedPair_IntArrayPair{
			IntArrayPair: &modelv1.IntArray{
				Value: convert.Int32ToInt64(v...),
			},
		}
	case int64:
		result.Typed = &modelv1.TypedPair_IntPair{
			IntPair: &modelv1.Int{
				Value: v,
			},
		}
	case []int64:
		result.Typed = &modelv1.TypedPair_IntArrayPair{
			IntArrayPair: &modelv1.IntArray{
				Value: v,
			},
		}
	case string:
		result.Typed = &modelv1.TypedPair_StrPair{
			StrPair: &modelv1.Str{
				Value: v,
			},
		}
	case []string:
		result.Typed = &modelv1.TypedPair_StrArrayPair{
			StrArrayPair: &modelv1.StrArray{
				Value: v,
			},
		}
	}
	return result
}

type FieldEntry struct {
	Key   string
	Index int
	Type  databasev1.FieldType
}
