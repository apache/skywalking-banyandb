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

package encoding_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

func TestInt64ListToBytes(t *testing.T) {
	testCases := []struct {
		name           string
		mt             encoding.EncodeType
		firstValue     int64
		values         []int64
		expectedDstLen int
	}{
		{
			name:       "EncodeTypeDelta",
			mt:         encoding.EncodeTypeDelta,
			firstValue: 0,
			values:     []int64{0, 2, 1, 3, 4},
		},
		{
			name:       "EncodeTypeDeltaOfDelta",
			mt:         encoding.EncodeTypeDeltaOfDelta,
			firstValue: 0,
			values:     []int64{0, 1, 4, 6, 9},
		},
		{
			name:       "EncodeTypeConst",
			mt:         encoding.EncodeTypeConst,
			firstValue: 0,
			values:     []int64{0, 0, 0, 0, 0},
		},
		{
			name:       "EncodeTypeDeltaConst",
			mt:         encoding.EncodeTypeDeltaConst,
			firstValue: 0,
			values:     []int64{0, 1, 2, 3, 4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, 0)
			var encodeType encoding.EncodeType
			var firstValue int64
			dst, encodeType, firstValue = encoding.Int64ListToBytes(dst, tc.values)
			require.Equal(t, tc.mt, encodeType, "EncodeType should be equal to the original EncodeType")
			require.Equal(t, tc.firstValue, firstValue, "FirstValue should be equal to the original FirstValue")

			decoded := make([]int64, 0, len(tc.values))
			decoded, err := encoding.BytesToInt64List(decoded, dst, tc.mt, tc.firstValue, len(tc.values))
			require.NoError(t, err, "BytesToInt64List should not return an error")
			require.Equal(t, tc.values, decoded, "Decoded int64s should be equal to the original int64s")
		})
	}
}
