// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package hll

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randomValues(seed int64, n int) []uint64 {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test data
	values := make([]uint64, n)
	for i := range values {
		values[i] = rng.Uint64()
	}
	return values
}

func TestEmpty(t *testing.T) {
	assert.Equal(t, uint64(0), New().Estimate())
}

func TestSingleValue(t *testing.T) {
	for _, v := range []uint64{0, 1, 1 << 63, math.MaxUint64, 0x9e3779b97f4a7c15} {
		s := New()
		s.Add(v)
		assert.Equal(t, uint64(1), s.Estimate(), "value %#x", v)
	}
}

func TestSigmaTauBoundaries(t *testing.T) {
	assert.Zero(t, sigma(0))
	assert.Zero(t, tau(0))
	assert.Zero(t, tau(1))
	assert.Greater(t, sigma(0.5), 0.5, "sigma adds the series terms to x")
	assert.Greater(t, tau(0.5), 0.0)
	assert.Less(t, tau(0.5), 1.0)
}

// TestEveryRegisterAtMaxRank fills every register with the largest rank, which no uniform hash does in practice. The
// estimate is then not finite and must be capped instead of converted from infinity.
func TestEveryRegisterAtMaxRank(t *testing.T) {
	viaAdd := New()
	for i := uint64(0); i < registerCount; i++ {
		viaAdd.Add(i << (64 - Precision)) // the register i, with no bit set after the index: the rank is maxRank
	}
	for i := range viaAdd.registers {
		require.EqualValues(t, maxRank, viaAdd.registers[i].Load())
	}
	assert.Equal(t, uint64(math.MaxUint64), viaAdd.Estimate())

	direct := New()
	for i := range direct.registers {
		direct.registers[i].Store(maxRank)
	}
	assert.Equal(t, uint64(math.MaxUint64), direct.Estimate())
}

func TestAccuracy(t *testing.T) {
	const (
		seeds        = 20
		maxMeanBias  = 0.005
		maxSingleErr = 0.03
	)
	for _, n := range []int{100, 1_000, 5_000, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 80_000, 100_000, 1_000_000} {
		var sum, maxErr float64
		for seed := int64(1); seed <= seeds; seed++ {
			s := New()
			for _, v := range randomValues(seed, n) {
				s.Add(v)
			}
			rel := (float64(s.Estimate()) - float64(n)) / float64(n)
			sum += rel
			maxErr = math.Max(maxErr, math.Abs(rel))
		}
		mean := sum / seeds
		t.Logf("n=%d mean_bias=%+.4f max_abs_err=%.4f", n, mean, maxErr)
		// The estimate is an integer, so tiny cardinalities cannot be more precise than one element.
		assert.Less(t, math.Abs(mean), math.Max(maxMeanBias, 1/float64(n)), "n=%d mean relative bias", n)
		assert.Less(t, maxErr, maxSingleErr, "n=%d max relative error", n)
	}
}

func TestOrderAndDuplicatesDoNotMatter(t *testing.T) {
	values := randomValues(7, 50_000)
	want := New()
	for _, v := range values {
		want.Add(v)
	}

	shuffled := append([]uint64(nil), values...)
	rand.New(rand.NewSource(1)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] }) //nolint:gosec // deterministic test data
	got := New()
	for _, v := range shuffled {
		got.Add(v)
	}
	for _, v := range values {
		got.Add(v)
	}
	assert.Equal(t, want.Estimate(), got.Estimate())
}

func TestReset(t *testing.T) {
	s := New()
	for _, v := range randomValues(3, 1000) {
		s.Add(v)
	}
	assert.NotZero(t, s.Estimate())
	s.Reset()
	assert.Equal(t, uint64(0), s.Estimate())
}

func TestConcurrent(t *testing.T) {
	const workers = 8
	values := randomValues(11, 80_000)
	want := New()
	for _, v := range values {
		want.Add(v)
	}

	s := New()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test helper goroutine
		go func(w int) {
			defer wg.Done()
			for i, v := range values {
				s.Add(v)
				if i%1000 == w {
					_ = s.Estimate()
				}
			}
		}(w)
	}
	wg.Wait()
	assert.Equal(t, want.Estimate(), s.Estimate())
}

func TestAddDoesNotAllocate(t *testing.T) {
	s := New()
	values := randomValues(5, 1024)
	i := 0
	allocs := testing.AllocsPerRun(1000, func() {
		s.Add(values[i%len(values)])
		i++
	})
	assert.Zero(t, allocs)
}

func BenchmarkAdd(b *testing.B) {
	values := randomValues(1, 1<<20)
	b.Run("cold", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			s := New()
			i := 0
			for pb.Next() {
				s.Add(values[i&(len(values)-1)])
				i++
			}
		})
	})
	b.Run("steady", func(b *testing.B) {
		s := New()
		for _, v := range values {
			s.Add(v)
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				s.Add(values[i&(len(values)-1)])
				i++
			}
		})
	})
}

func BenchmarkEstimate(b *testing.B) {
	s := New()
	for _, v := range randomValues(1, 100_000) {
		s.Add(v)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.Estimate()
	}
}
