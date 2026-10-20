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

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

const benchStrRows = 8192

// benchStrCases are the 8,192-row string tag columns the borrowed fill is measured on.
var benchStrCases = []struct {
	gen  func(i int) string
	name string
}{
	// Short entity-style values, one distinct value per row: plain encoding.
	{name: "short-plain", gen: func(i int) string { return fmt.Sprintf("svc-%06d", i) }},
	// Short values with few distinct ones: dictionary encoding.
	{name: "short-dict", gen: func(i int) string { return fmt.Sprintf("svc-%02d", i%40) }},
	// Longer endpoint-style values (~43 bytes), unique per row: plain encoding.
	{name: "endpoint-plain", gen: func(i int) string { return fmt.Sprintf("/api/v1/tenants/%05d/orders/%07d/items", i%900, i) }},
}

// BenchmarkStringTagFill measures decoding one encoded string tag column into a
// cursor and filling a batch column from it, per 8,192-row block. "copy" is the
// per-value string allocation fillTagRows does; "borrow" is borrowTagRows, which
// builds string headers over the decoded bytes and pins the cursor.
func BenchmarkStringTagFill(b *testing.B) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "t", Type: vectorized.ColumnTypeString},
	})
	def := schema.Columns[0]
	for _, tc := range benchStrCases {
		values := make([][]byte, benchStrRows)
		for i := range values {
			values[i] = []byte(tc.gen(i))
		}
		src := &column{name: "t", valueType: pbv1.ValueTypeStr, values: values}
		cm := &columnMetadata{}
		buf := &bytes.Buffer{}
		w := &writer{}
		w.init(buf)
		src.mustWriteTo(cm, w)

		for _, mode := range []string{"copy", "borrow"} {
			b.Run(tc.name+"/"+mode, func(b *testing.B) {
				bc := generateBlockCursor()
				var col column
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					bc.columnValuesDecoder.Reset()
					col.reset()
					col.mustReadValues(&bc.columnValuesDecoder, buf, *cm, benchStrRows, true)
					mb := newMeasureBatchForSchema(schema, benchStrRows)
					if mode == "borrow" {
						bc.borrowTagRows(mb, mb.Tags[0], def, &col, 0, benchStrRows, false)
					} else {
						fillTagRows(mb.Tags[0], def, &col, 0, benchStrRows, false)
					}
					mb.Release()
				}
			})
		}
	}
}
