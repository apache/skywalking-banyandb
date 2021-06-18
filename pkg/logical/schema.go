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

package logical

import (
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

type fieldSchema struct {
	fieldMap map[string]int
	fields   []*apiv1.FieldSpec
}

func (fs *fieldSchema) RegisterField(name string, i int, spec *apiv1.FieldSpec) {
	fs.fieldMap[name] = i
	fs.fields = append(fs.fields, spec)
}

func (fs *fieldSchema) FieldDefined(name string) bool {
	if _, ok := fs.fieldMap[name]; ok {
		return true
	}
	return false
}

func (fs *fieldSchema) CreateRef(name string) (Expr, error) {
	if idx, ok := fs.fieldMap[name]; ok {
		return NewFieldRef(name, idx, fs.fields[idx].Type()), nil
	}
	return nil, FieldNotDefinedErr
}
