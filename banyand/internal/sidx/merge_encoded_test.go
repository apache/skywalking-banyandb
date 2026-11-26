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

package sidx

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func encodedArrayMergeFixture(tb testing.TB, mode string) ([]*part, [][]byte, [][]byte) {
	tb.Helper()
	var strings, integers [][]byte
	for i := range 40 {
		strings = append(strings, []byte(fmt.Sprintf("tag-%02d=a|b\\c", i)))
		integers = append(integers, convert.Int64ToBytes(int64(i)-20))
	}
	var parts []*part
	for partIndex := range 2 {
		es := generateElements()
		for row := range 512 {
			stringValues, integerValues := strings, integers
			if mode == "distinct" {
				stringValues = append([][]byte(nil), strings...)
				integerValues = append([][]byte(nil), integers...)
				stringValues[0] = []byte(fmt.Sprintf("row=%d|\\", row*2+partIndex))
				integerValues[0] = convert.Int64ToBytes(int64(row*2 + partIndex))
			}
			tags := []Tag{
				{Name: "query", ValueArr: stringValues, ValueType: pbv1.ValueTypeStrArr},
				{Name: "numbers", ValueArr: integerValues, ValueType: pbv1.ValueTypeInt64Arr},
			}
			if mode == "scalar" {
				tags = []Tag{
					{Name: "query", Value: stringValues[0], ValueType: pbv1.ValueTypeStr},
					{Name: "numbers", Value: integerValues[0], ValueType: pbv1.ValueTypeInt64},
				}
			}
			es.mustAppend(1, int64(row*2+partIndex), []byte("trace"), tags)
		}
		mp := GenerateMemPart()
		mp.mustInitFromElements(es)
		releaseElements(es)
		p := openMemPart(mp)
		parts = append(parts, p)
		tb.Cleanup(func() { p.close(); ReleaseMemPart(mp) })
	}
	return parts, strings, integers
}

func mergeEncodedArrayFixture(tb testing.TB, parts []*part) *memPart {
	tb.Helper()
	iters := make([]*partMergeIter, len(parts))
	for i, p := range parts {
		iters[i] = generatePartMergeIter()
		iters[i].mustInitFromPart(p)
	}
	br := generateBlockReader()
	br.init(iters)
	out := GenerateMemPart()
	bw := generateBlockWriter()
	bw.MustInitForMemPart(out)
	metadata, err := mergeBlocks(nil, bw, br, nil, nil)
	releaseBlockWriter(bw)
	releaseBlockReader(br)
	for _, iter := range iters {
		releasePartMergeIter(iter)
	}
	require.NoError(tb, err)
	out.partMetadata = metadata
	return out
}

func TestMergeRepeatedArraysAllocationBudget(t *testing.T) {
	parts, strings, integers := encodedArrayMergeFixture(t, "repeated")
	allocations := testing.AllocsPerRun(5, func() {
		out := mergeEncodedArrayFixture(t, parts)
		ReleaseMemPart(out)
	})
	// Repeated arrays should be decoded once per distinct value, rather than
	// allocate element slices and encoding buffers again for every input row.
	require.Less(t, allocations, float64(2048), "merge allocated %.0f objects for 1024 repeated arrays", allocations)
	out := mergeEncodedArrayFixture(t, parts)
	defer ReleaseMemPart(out)
	require.Equal(t, uint64(1024), out.partMetadata.TotalCount)
	require.Equal(t, parts[0].partMetadata.UncompressedSizeBytes+parts[1].partMetadata.UncompressedSizeBytes, out.partMetadata.UncompressedSizeBytes)
	p := openMemPart(out)
	defer p.close()
	decoded, err := p.readAll()
	require.NoError(t, err)
	var count int
	for _, es := range decoded {
		for row, key := range es.userKeys {
			require.Equal(t, int64(count), key)
			require.Len(t, es.tags[row], 2)
			for _, tag := range es.tags[row] {
				values, decodeErr := unmarshalTag(nil, tag.value, tag.valueType)
				require.NoError(t, decodeErr)
				switch tag.name {
				case "query":
					require.Equal(t, strings, values)
				case "numbers":
					require.Equal(t, integers, values)
				default:
					t.Fatalf("unexpected tag %q", tag.name)
				}
			}
			count++
		}
		releaseElements(es)
	}
	require.Equal(t, 1024, count)
}

func BenchmarkMergeRepeatedArrays(b *testing.B) {
	benchmarkMergeTags(b, "repeated")
}

func BenchmarkMergeDistinctArrays(b *testing.B) {
	benchmarkMergeTags(b, "distinct")
}

func BenchmarkMergeScalarTags(b *testing.B) {
	benchmarkMergeTags(b, "scalar")
}

func benchmarkMergeTags(b *testing.B, mode string) {
	parts, _, _ := encodedArrayMergeFixture(b, mode)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out := mergeEncodedArrayFixture(b, parts)
		ReleaseMemPart(out)
	}
}
