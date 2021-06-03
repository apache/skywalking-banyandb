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

package types

var _ Field = (*field)(nil)

type FieldType uint8

const (
	STRING FieldType = iota
	INT64
	BOOLEAN
	ARRAY_STRING
	ARRAY_INT64
)

//go:generate mockery --name Field --output ../internal/mocks
type Field interface {
	Name() string
	Type() FieldType
}

type field struct {
	name string
	typ  FieldType
}

func (f field) Name() string {
	return f.name
}

func (f field) Type() FieldType {
	return f.typ
}

func NewField(fieldName string, typ FieldType) Field {
	return &field{fieldName, typ}
}
