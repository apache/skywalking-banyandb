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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	binaryOpsMap = map[string]v1.PairQuery_BinaryOp{
		"=":          v1.PairQuery_BINARY_OP_EQ,
		"!=":         v1.PairQuery_BINARY_OP_NE,
		">":          v1.PairQuery_BINARY_OP_GT,
		">=":         v1.PairQuery_BINARY_OP_GE,
		"<":          v1.PairQuery_BINARY_OP_LT,
		"<=":         v1.PairQuery_BINARY_OP_LE,
		"having":     v1.PairQuery_BINARY_OP_HAVING,
		"not having": v1.PairQuery_BINARY_OP_NOT_HAVING,
	}
)

type entityCriteriaBuilder struct {
	ec *v1.EntityCriteria
}

func NewEntityCriteriaBuilder() *entityCriteriaBuilder {
	return &entityCriteriaBuilder{
		ec: &v1.EntityCriteria{},
	}
}

func (b *entityCriteriaBuilder) Metadata(group, name string) *entityCriteriaBuilder {
	b.ec.Metadata = &v1.Metadata{
		Group: group,
		Name:  name,
	}
	return b
}

func (b *entityCriteriaBuilder) Limit(limit uint32) *entityCriteriaBuilder {
	b.ec.Limit = limit
	return b
}

func (b *entityCriteriaBuilder) Offset(offset uint32) *entityCriteriaBuilder {
	b.ec.Offset = offset
	return b
}

func (b *entityCriteriaBuilder) Fields(items ...interface{}) *entityCriteriaBuilder {
	if len(items)%3 != 0 {
		panic("expect even number of arguments")
	}

	b.ec.Fields = make([]*v1.PairQuery, len(items)/3)
	for i := 0; i < len(items)/3; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		b.ec.Fields[i] = &v1.PairQuery{
			Op:        binaryOpsMap[op.(string)],
			Condition: buildPair(key.(string), values),
		}
	}

	return b
}

func buildPair(key string, value interface{}) *v1.TypedPair {
	switch v := value.(type) {
	case int:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: []int64{int64(v)},
				},
			},
		}
	case []int:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: convert.IntToInt64(v...),
				},
			},
		}
	case int8:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: []int64{int64(v)},
				},
			},
		}
	case []int8:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: convert.Int8ToInt64(v...),
				},
			},
		}
	case int16:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: []int64{int64(v)},
				},
			},
		}
	case []int16:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: convert.Int16ToInt64(v...),
				},
			},
		}
	case int32:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: []int64{int64(v)},
				},
			},
		}
	case []int32:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: convert.Int32ToInt64(v...),
				},
			},
		}
	case int64:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: []int64{v},
				},
			},
		}
	case []int64:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_IntPair{
				IntPair: &v1.IntPair{
					Key:    key,
					Values: v,
				},
			},
		}
	case string:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_StrPair{
				StrPair: &v1.StrPair{
					Key:    key,
					Values: []string{v},
				},
			},
		}
	case []string:
		return &v1.TypedPair{
			Typed: &v1.TypedPair_StrPair{
				StrPair: &v1.StrPair{
					Key:    key,
					Values: v,
				},
			},
		}
	default:
		panic("not supported value type")
	}
}

func (b *entityCriteriaBuilder) Projection(projections ...string) *entityCriteriaBuilder {
	b.ec.Projection = &v1.Projection{KeyNames: projections}
	return b
}

func (b *entityCriteriaBuilder) OrderBy(fieldName string, sort v1.QueryOrder_Sort) *entityCriteriaBuilder {
	b.ec.OrderBy = &v1.QueryOrder{
		KeyName: fieldName,
		Sort:    sort,
	}
	return b
}

func (b *entityCriteriaBuilder) TimeRange(sT, eT time.Time) *entityCriteriaBuilder {
	b.ec.TimeRange = &v1.TimeRange{
		Begin: timestamppb.New(sT),
		End:   timestamppb.New(eT),
	}
	return b
}

func (b *entityCriteriaBuilder) Build() *v1.EntityCriteria {
	return b.ec
}

type queryEntityBuilder struct {
	qe *v1.Entity
}

func NewQueryEntityBuilder() *queryEntityBuilder {
	return &queryEntityBuilder{qe: &v1.Entity{}}
}

func (qeb *queryEntityBuilder) EntityID(entityID string) *queryEntityBuilder {
	qeb.qe.EntityId = entityID
	return qeb
}

func (qeb *queryEntityBuilder) Timestamp(t time.Time) *queryEntityBuilder {
	qeb.qe.Timestamp = timestamppb.New(t)
	return qeb
}

func (qeb *queryEntityBuilder) Fields(items ...interface{}) *queryEntityBuilder {
	if len(items)%2 != 0 {
		panic("invalid fields list")
	}

	l := len(items) / 2

	qeb.qe.Fields = make([]*v1.TypedPair, l)
	for i := 0; i < l; i++ {
		key, values := items[i*2+0], items[i*2+1]
		qeb.qe.Fields[i] = buildPair(key.(string), values)
	}

	return qeb
}

func (qeb *queryEntityBuilder) Build() *v1.Entity {
	return qeb.qe
}
