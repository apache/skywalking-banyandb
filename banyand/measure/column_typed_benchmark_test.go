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
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BenchmarkColumnDecodeFill measures what loading one 8,192-row numeric column
// costs on the query path: decode the stored block into a pooled block column,
// copy the rows into the cursor column (as loadData does), then fill a native
// batch column. "bytes" is the per-row []byte form every query used before;
// "typed" decodes straight into a typed slice.
//
// Run it with -bench BenchmarkColumnDecodeFill -benchmem -count=6.
func BenchmarkColumnDecodeFill(b *testing.B) {
	const rows = 8192
	for _, vt := range []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeFloat64} {
		typeName := "int64"
		if vt == pbv1.ValueTypeFloat64 {
			typeName = "float64"
		}
		for _, kind := range []string{"counter", "gauge"} {
			src := &column{name: "c", valueType: vt, values: genTypedValues(vt, kind, rows, false)}
			cm := &columnMetadata{}
			buf := &bytes.Buffer{}
			w := &writer{}
			w.init(buf)
			src.mustWriteTo(cm, w)
			for _, typed := range []bool{false, true} {
				mode := "bytes"
				if typed {
					mode = "typed"
				}
				b.Run(fmt.Sprintf("%s/%s/%s", typeName, kind, mode), func(b *testing.B) {
					def := vectorized.ColumnDef{Role: vectorized.RoleField, Name: "c"}
					var dst vectorized.Column
					if vt == pbv1.ValueTypeInt64 {
						def.Type = vectorized.ColumnTypeInt64
						dst = vectorized.NewInt64Column(rows)
					} else {
						def.Type = vectorized.ColumnTypeFloat64
						dst = vectorized.NewFloat64Column(rows)
					}
					decoder := &encoding.BytesBlockDecoder{}
					tmp := &column{}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						tmp.reset()
						tmp.mustReadValues(decoder, buf, *cm, rows, typed)
						cursorCol := column{valueType: vt}
						cursorCol.copyRowsFrom(tmp, 0, rows)
						dst.Reset()
						fillFieldRows(dst, def, &cursorCol, 0, rows, false)
					}
					b.StopTimer()
					require.Equal(b, rows, dst.Len())
				})
			}
		}
	}
}
