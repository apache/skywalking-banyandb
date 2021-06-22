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

package grpc_test

import (
	flatbuffers "github.com/google/flatbuffers/go"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/stretchr/testify/assert"
	"testing"
)

type ComponentBuilderFunc func(*flatbuffers.Builder)
type criteriaBuilder struct {
	*flatbuffers.Builder
}
func NewCriteriaBuilder() *criteriaBuilder {
	return &criteriaBuilder{
		flatbuffers.NewBuilder(1024),
	}
}

func (b *criteriaBuilder) Build(funcs ...ComponentBuilderFunc) *v1.WriteEntity {
	v1.WriteEntityStart(b.Builder)
	for _, fun := range funcs {
		fun(b.Builder)
	}
	entityOffset := v1.WriteEntityEnd(b.Builder)
	b.Builder.Finish(entityOffset)

	buf := b.Bytes[b.Head():]
	return v1.GetRootAsWriteEntity(buf, 0)
}
func BenchmarkWriteTraces(t *testing.T) {
	tester := assert.New(t)
	builder := NewCriteriaBuilder()
	entity := builder.Build(
	)
	res := grpc.WriteTraces(entity)
	//tester.NoError(err)
	tester.NotNil(res)
	//tester.NoError(plan.Validate())
}

func BenchmarkReadTraces(t *testing.T) {
	tester := assert.New(t)
	builder := NewCriteriaBuilder()
	entity := builder.Build()
	res := grpc.ReadTraces(entity)
	tester.NotNil(res)
}