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

package frame

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// benchBatch is the 4-column, 8,192-row batch of the batch v2 design: a timestamp, two string tags
// and a float field, with no nulls and no constant column.
func benchBatch(rows int) *vectorized.RecordBatch {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Name: "_timestamp", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "service", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleTag, TagFamily: "default", Name: "endpoint", Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: "latency", Type: vectorized.ColumnTypeFloat64},
	})
	b := vectorized.NewRecordBatch(schema, rows)
	for i := range rows {
		b.Columns[0].(*vectorized.TypedColumn[int64]).Append(1_700_000_000_000_000_000 + int64(i)*1_000_000)
		b.Columns[1].(*vectorized.TypedColumn[string]).Append(fmt.Sprintf("service-%04d.namespace.svc.cluster.local", i%97))
		b.Columns[2].(*vectorized.TypedColumn[string]).Append(fmt.Sprintf("/api/v1/resource/%d/action", i%1013))
		b.Columns[3].(*vectorized.TypedColumn[float64]).Append(float64(i%2000) * 0.37)
	}
	b.Len = rows
	return b
}

var benchVersions = []struct {
	name    string
	version uint8
}{{"v1", WireVersion}, {"v2", WireVersionColumnar}}

func BenchmarkFrameEncode(b *testing.B) {
	batch := benchBatch(8192)
	for _, v := range benchVersions {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			var size int
			for range b.N {
				body, err := EncodeVersion(batch, v.version)
				if err != nil {
					b.Fatal(err)
				}
				size = len(body)
			}
			b.ReportMetric(float64(size), "frameB")
		})
	}
}

func BenchmarkFrameDecode(b *testing.B) {
	batch := benchBatch(8192)
	for _, v := range benchVersions {
		body, err := EncodeVersion(batch, v.version)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			retained := retainedAfterDecode(body)
			b.ResetTimer()
			for range b.N {
				decoded, decodeErr := Decode(body)
				if decodeErr != nil {
					b.Fatal(decodeErr)
				}
				decoded.ReleaseOwners()
			}
			b.ReportMetric(float64(len(body)), "frameB")
			b.ReportMetric(retained, "retainedB")
		})
	}
}

// retainedAfterDecode is the heap a decoded batch keeps alive besides the message itself: the
// message is allocated first and counted out, so a columnar batch that borrows it shows only its
// own headers, and the row-oriented batch shows every string and slice it copied.
func retainedAfterDecode(body []byte) float64 {
	const rounds = 5
	var total float64
	for range rounds {
		msg := append([]byte(nil), body...)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		decoded, err := Decode(msg)
		if err != nil {
			panic(err)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		total += float64(after.HeapAlloc) - float64(before.HeapAlloc)
		runtime.KeepAlive(decoded)
		runtime.KeepAlive(msg)
		decoded.ReleaseOwners()
	}
	return total / rounds
}
