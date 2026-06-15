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

package trace

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeInt64SortKeyPreservesNumericOrder(t *testing.T) {
	values := []int64{0, -1, 1, math.MinInt64, math.MaxInt64, -100, 100}
	sort.Slice(values, func(i, j int) bool {
		left := encodeInt64SortKey(values[i])
		right := encodeInt64SortKey(values[j])
		return bytes.Compare(left[:], right[:]) < 0
	})
	require.Equal(t, []int64{math.MinInt64, -100, -1, 0, 1, 100, math.MaxInt64}, values)
}
