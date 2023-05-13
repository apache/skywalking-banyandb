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

package run

import (
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// Bytes is a custom type to store memory size in bytes.
type Bytes int64

// String returns a string representation of the Bytes value.
func (b Bytes) String() string {
	suffixes := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	size := float64(b)
	base := 1024.0
	if size < base {
		return fmt.Sprintf("%.0f%s", size, suffixes[0])
	}

	exp := int(math.Log(size) / math.Log(base))
	result := size / math.Pow(base, float64(exp))
	return fmt.Sprintf("%.2f%s", result, suffixes[exp])
}

// Set sets the Bytes value from the input string.
func (b *Bytes) Set(s string) error {
	size, err := convert.ParseSize(s)
	if err != nil {
		return err
	}
	*b = Bytes(size)
	return nil
}

// Type returns the type name of the Bytes custom type.
func (b *Bytes) Type() string {
	return "bytes"
}
