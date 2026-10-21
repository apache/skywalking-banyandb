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

package measure

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

func TestColumnEncodingPreservesReusableNumericScratch(t *testing.T) {
	for _, valueType := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		t.Run(fmt.Sprint(valueType), func(t *testing.T) {
			scratch := installColumnScratchPools(t)
			c := &column{valueType: valueType, values: genTypedValues(valueType, "gauge", 64, false)}
			encodePoolTestColumn(c)
			require.Equal(t, 8192, cap(*scratch.ints), "encoder discarded reusable int64 backing")
			require.Equal(t, 8192, cap(*scratch.floats), "encoder discarded reusable float64 backing")
			require.Zero(t, scratch.intPool.RefsCount())
			require.Zero(t, scratch.floatPool.RefsCount())
		})
	}
}

func TestColumnDecodeBoundsOversizedNumericScratch(t *testing.T) {
	for _, valueType := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		for _, typed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/typed=%t", valueType, typed), func(t *testing.T) {
				c := &column{valueType: valueType, values: genTypedValues(valueType, "gauge", 64, false)}
				encoded := encodePoolTestColumn(c)
				scratch := installColumnScratchPools(t)
				decoded := &column{valueType: valueType}
				decoded.decodeColumnValues(&encoding.BytesBlockDecoder{}, "test", 64, encoded, typed)
				require.Equal(t, len(c.values), decoded.rowCount())
				for idx, value := range c.values {
					require.Equal(t, value, decoded.rowBytes(idx), "row %d", idx)
				}
				if valueType == pbv1.ValueTypeFloat64 || !typed {
					require.LessOrEqual(t, cap(*scratch.ints), 2048)
				}
				if valueType == pbv1.ValueTypeFloat64 && !typed {
					require.LessOrEqual(t, cap(*scratch.floats), 2048)
				}
				require.Zero(t, scratch.intPool.RefsCount())
				require.Zero(t, scratch.floatPool.RefsCount())
			})
		}
	}
}

func BenchmarkColumnEncodeAlternatingSizes(b *testing.B) {
	for _, valueType := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		b.Run(fmt.Sprint(valueType), func(b *testing.B) {
			large := &column{valueType: valueType, values: genTypedValues(valueType, "gauge", 8192, false)}
			small := &column{valueType: valueType, values: large.values[:64]}
			buffer := &bytes.Buffer{Buf: make([]byte, 0, 81920)}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if valueType == pbv1.ValueTypeInt64 {
					large.encodeInt64Column(buffer)
					small.encodeInt64Column(buffer)
				} else {
					large.encodeFloat64Column(buffer)
					small.encodeFloat64Column(buffer)
				}
			}
		})
	}
}

type columnScratchPools struct {
	ints      *[]int64
	floats    *[]float64
	intPool   *pool.Synced[*[]int64]
	floatPool *pool.Synced[*[]float64]
}

func installColumnScratchPools(t *testing.T) columnScratchPools {
	t.Helper()
	previousInts, previousFloats := int64SlicePool, float64SlicePool
	ints, floats := make([]int64, 8192), make([]float64, 8192)
	scratch := columnScratchPools{
		ints: &ints, floats: &floats,
		intPool: &pool.Synced[*[]int64]{}, floatPool: &pool.Synced[*[]float64]{},
	}
	scratch.intPool.New = func() any { return scratch.ints }
	scratch.floatPool.New = func() any { return scratch.floats }
	int64SlicePool, float64SlicePool = scratch.intPool, scratch.floatPool
	t.Cleanup(func() { int64SlicePool, float64SlicePool = previousInts, previousFloats })
	return scratch
}

func encodePoolTestColumn(c *column) *bytes.Buffer {
	buffer := &bytes.Buffer{}
	if c.valueType == pbv1.ValueTypeInt64 {
		c.encodeInt64Column(buffer)
	} else {
		c.encodeFloat64Column(buffer)
	}
	return buffer
}
