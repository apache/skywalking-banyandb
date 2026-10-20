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
	"sync"
	"testing"
)

// BenchmarkVectorizedStorageConcurrent runs the W2, W9, W10 and W11 shapes with
// several queries at once, the way a data node serves concurrent dashboards. One
// op is a round of n queries started together; the decode pool is shared by all
// of them.
func BenchmarkVectorizedStorageConcurrent(b *testing.B) {
	hs := newGateHarnesses(b)
	for _, w := range gateWorkloads() {
		switch w.id {
		case "W2", "W9", "W11":
		default:
			continue
		}
		for _, n := range []int{1, 8} {
			b.Run(w.id+"x"+string(rune('0'+n)), func(b *testing.B) {
				h := hs.get(w.layout)
				b.ReportAllocs()
				b.ResetTimer()
				sink := 0
				var mu sync.Mutex
				for b.Loop() {
					var wg sync.WaitGroup
					for range n {
						wg.Add(1)
						go func() {
							defer wg.Done()
							rows := runGateWorkload(h, w, false)
							mu.Lock()
							sink += rows
							mu.Unlock()
						}()
					}
					wg.Wait()
				}
				gateSink = sink
			})
		}
	}
}
