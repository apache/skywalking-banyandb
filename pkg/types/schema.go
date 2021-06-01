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

var _ Schema = (*schema)(nil)

type schema struct {
	fields []Field
}

func (s schema) Select(names ...string) Schema {
	var filteredFields []Field
	namesMap := make(map[string]struct{})
	for _, name := range names {
		namesMap[name] = struct{}{}
	}
	for _, f := range s.fields {
		if _, ok := namesMap[f.Name()]; ok {
			filteredFields = append(filteredFields, f)
		}
	}
	return NewSchema(filteredFields...)
}

func (s schema) GetFields() []Field {
	return s.fields
}

func NewSchema(fields ...Field) Schema {
	return &schema{
		fields: fields,
	}
}

//go:generate mockery --name Schema --output ../internal/mocks
type Schema interface {
	Select(names ...string) Schema
	GetFields() []Field
}
