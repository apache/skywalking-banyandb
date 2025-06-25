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
	"math/rand"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func benchmarkColumnEncoding[T any](
	b *testing.B,
	dataTypeName string,
	generateNames []string,
	sizes []int,
	generateFn func(string, int) []T,
	convertFn func(T) []byte,
	valueType pbv1.ValueType,
) {
	for _, name := range generateNames {
		for _, size := range sizes {
			rawValues := generateFn(name, size)

			newBytes := make([][]byte, size)
			oldBytes := make([][]byte, size)
			for i, v := range rawValues {
				newBytes[i] = convertFn(v)
				oldBytes[i] = []byte(fmt.Sprintf("%v", v))
			}

			benchCases := []struct {
				label     string
				values    [][]byte
				valueType pbv1.ValueType
			}{
				{"old_str", oldBytes, pbv1.ValueTypeStr},
				{fmt.Sprintf("new_%s", dataTypeName), newBytes, valueType},
			}

			for _, bc := range benchCases {
				b.Run(fmt.Sprintf("%s_%s_%d", name, bc.label, size), func(b *testing.B) {
					original := &column{
						name:      dataTypeName + "_test",
						valueType: bc.valueType,
						values:    bc.values,
					}

					var totalEncodeTimeNs, totalDecodeTimeNs int64

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						buf := &bytes.Buffer{}
						cm := &columnMetadata{}
						w := &writer{}
						w.init(buf)

						b.StopTimer()
						startEncode := time.Now()
						b.StartTimer()

						original.mustWriteTo(cm, w)

						b.StopTimer()
						totalEncodeTimeNs += time.Since(startEncode).Nanoseconds()
						b.StartTimer()

						encodedSize := len(buf.Bytes())
						rawSize := size * 8
						b.ReportMetric(float64(encodedSize)/float64(rawSize), "compression_ratio")

						b.StopTimer()
						startDecode := time.Now()
						b.StartTimer()

						unmarshaled := &column{}
						decoder := &encoding.BytesBlockDecoder{}
						unmarshaled.mustReadValues(decoder, buf, *cm, uint64(size))

						b.StopTimer()
						totalDecodeTimeNs += time.Since(startDecode).Nanoseconds()
						b.StartTimer()

						if len(unmarshaled.values) != size {
							b.Fatalf("mismatch size: got %d, want %d", len(unmarshaled.values), size)
						}
					}

					b.ReportMetric(float64(totalEncodeTimeNs)/float64(b.N), "encode_time_ns/op")
					b.ReportMetric(float64(totalDecodeTimeNs)/float64(b.N), "decode_time_ns/op")
					b.ReportMetric(float64(totalEncodeTimeNs+totalDecodeTimeNs)/float64(b.N), "encode_decode_time_ns/op")
				})
			}
		}
	}
}

// #nosec G404
func generateDataFloat64(name string, n int) []float64 {
	switch name {
	case "const":
		return make([]float64, n)
	case "incrementing":
		out := make([]float64, n)
		for i := range out {
			out[i] = 1.0 + float64(i)
		}
		return out
	case "small_fluctuations":
		out := make([]float64, n)
		val := 25.0
		for i := range out {
			val += (rand.Float64() - 0.5) * 0.2
			out[i] = val
		}
		return out
	case "random":
		out := make([]float64, n)
		for i := range out {
			out[i] = rand.Float64() * 100
		}
		return out
	default:
		panic("unknown pattern: " + name)
	}
}

// #nosec G404
func generateDataInt64(name string, n int) []int64 {
	switch name {
	case "const":
		return make([]int64, n)
	case "deltaConst":
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i)
		}
		return out
	case "delta":
		out := make([]int64, n)
		val := int64(0)
		for i := 0; i < n; i++ {
			out[i] = val
			val += rand.Int63n(10) + 1
		}
		return out
	case "incremental":
		out := make([]int64, n)
		val := int64(0)
		resetCount := 0
		for i := 0; i < n; i++ {
			out[i] = val
			if resetCount < 2 && i > 0 && rand.Intn(10) == 0 {
				decrement := rand.Int63n(val/8 + 1)
				val -= decrement
				resetCount++
			} else {
				val += rand.Int63n(10) + 1
			}
			if val < 0 {
				val = 0
			}
		}
		return out
	case "small_fluctuations":
		out := make([]int64, n)
		val := int64(25)
		for i := range out {
			val += rand.Int63n(11) - 5
			out[i] = val
		}
		return out
	case "random":
		out := make([]int64, n)
		for i := range out {
			out[i] = rand.Int63n(100)
		}
		return out
	default:
		panic("unsupported generate type: " + name)
	}
}

func BenchmarkFloat64XOREncoding(b *testing.B) {
	benchmarkColumnEncoding[float64](
		b,
		"float64",
		[]string{"const", "incrementing", "small_fluctuations", "random"},
		[]int{10_000},
		generateDataFloat64,
		convert.Float64ToBytes,
		pbv1.ValueTypeFloat64,
	)
}

func BenchmarkInt64Encoding(b *testing.B) {
	benchmarkColumnEncoding[int64](
		b,
		"int64",
		[]string{"const", "deltaConst", "delta", "incremental", "small_fluctuations", "random"},
		[]int{10_000},
		generateDataInt64,
		convert.Int64ToBytes,
		pbv1.ValueTypeInt64,
	)
}
