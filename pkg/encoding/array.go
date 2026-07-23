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

package encoding

import (
	"github.com/apache/skywalking-banyandb/pkg/encoding/vararray"
)

// EntityDelimiter and Escape are re-exported from the dependency-free vararray
// leaf so existing encoding.EntityDelimiter / encoding.Escape usages are
// unchanged, while consumers that need only the var-array codec (e.g. the
// plugin SDK) can import the leaf without pulling in this package's logging deps.
const (
	// EntityDelimiter is the delimiter for entities in a variable-length array.
	EntityDelimiter = vararray.EntityDelimiter
	// Escape is the escape character for entities in a variable-length array.
	Escape = vararray.Escape
)

// MarshalVarArray marshals a byte slice into a variable-length array format,
// escaping delimiter and escape characters. It forwards to the vararray leaf.
func MarshalVarArray(dest, src []byte) []byte {
	return vararray.MarshalVarArray(dest, src)
}

// UnmarshalVarArray unmarshals a variable-length array from src starting at idx.
// It forwards to the vararray leaf; see vararray.UnmarshalVarArray for the
// in-place-mutation contract and return semantics.
func UnmarshalVarArray(src []byte, idx int) (int, int, error) {
	return vararray.UnmarshalVarArray(src, idx)
}
