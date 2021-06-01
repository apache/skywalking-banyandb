//
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
// Code generated by mockery (devel). DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"

	types "github.com/apache/skywalking-banyandb/pkg/types"
)

// Schema is an autogenerated mock type for the Schema type
type Schema struct {
	mock.Mock
}

// GetFields provides a mock function with given fields:
func (_m *Schema) GetFields() []types.Field {
	ret := _m.Called()

	var r0 []types.Field
	if rf, ok := ret.Get(0).(func() []types.Field); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]types.Field)
		}
	}

	return r0
}

// Select provides a mock function with given fields: names
func (_m *Schema) Select(names ...string) types.Schema {
	_va := make([]interface{}, len(names))
	for _i := range names {
		_va[_i] = names[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 types.Schema
	if rf, ok := ret.Get(0).(func(...string) types.Schema); ok {
		r0 = rf(names...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(types.Schema)
		}
	}

	return r0
}
