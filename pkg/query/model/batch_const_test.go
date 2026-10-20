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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestMeasureBatchConstFromUsesItsOwnStorageThenThePool(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "f", Name: "a", Type: vectorized.ColumnTypeString},
	})
	src := vectorized.NewStringColumn(2)
	src.Append("value")
	src.AppendNull()

	b := AcquireMeasureBatch(schema, 4)
	var embedded []*vectorized.ConstColumn
	for range maxEmbeddedConsts {
		c, ok := b.ConstFrom(src, 0, 7)
		require.True(t, ok)
		require.Equal(t, "value", c.Str())
		require.Equal(t, 7, c.Len())
		embedded = append(embedded, c)
	}
	pooled, ok := b.ConstFrom(src, 1, 3)
	require.True(t, ok, "past the embedded storage a constant comes from the pool")
	require.True(t, pooled.IsNullValue())
	for _, c := range embedded {
		require.NotSame(t, pooled, c)
	}
	_, ok = b.ConstFrom(vectorized.NewInt64ArrayColumn(1), 0, 1)
	require.False(t, ok)

	// A constant in Tags is released with the batch; an embedded one is reset, not pooled.
	vectorized.ReleaseColumn(b.Tags[0])
	b.Tags[0] = embedded[0]
	b.Tags = append(b.Tags, pooled)
	b.Release()
	require.Zero(t, embedded[0].Len())
	require.Empty(t, embedded[0].Str())

	again := AcquireMeasureBatch(schema, 4)
	defer again.Release()
	c, ok := again.ConstFrom(src, 0, 2)
	require.True(t, ok)
	require.Equal(t, "value", c.Str())
}
