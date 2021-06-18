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
	"fmt"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ Expr = (*fieldRef)(nil)

// fieldRef is the reference to the field
// also it hold the definition/schema of the field
type fieldRef struct {
	// name defines the key of the field
	name string
	// index is used to access the data quickly
	index int
	// typ defines the data type for the field
	typ apiv1.FieldType
}

func (f *fieldRef) String() string {
	return fmt.Sprintf("#%s<%s>", f.name, apiv1.EnumNamesFieldType[f.typ])
}

func NewFieldRef(fieldName string, fieldIndex int, fieldType apiv1.FieldType) Expr {
	return &fieldRef{
		name:  fieldName,
		index: fieldIndex,
		typ:   fieldType,
	}
}
