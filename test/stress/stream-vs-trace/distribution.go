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

package streamvstrace

import (
	"crypto/rand"
	"math"
	"math/big"
)

// ZipfGenerator generates values following a Zipfian distribution.
// This is useful for simulating realistic data patterns where some values
// are much more common than others (e.g., 80/20 rule).
type ZipfGenerator struct {
	n    int     // number of possible values
	s    float64 // skewness parameter (s > 1, higher = more skewed)
	zeta float64 // normalization constant
}

// NewZipfGenerator creates a new Zipfian generator
// n: number of possible values (0 to n-1)
// s: skewness parameter (s > 1, higher values = more skewed distribution)
func NewZipfGenerator(n int, s float64) *ZipfGenerator {
	if s <= 1.0 {
		s = 1.1 // minimum valid value
	}

	// Calculate normalization constant (Riemann zeta function)
	zeta := 0.0
	for i := 1; i <= n; i++ {
		zeta += 1.0 / math.Pow(float64(i), s)
	}

	return &ZipfGenerator{
		n:    n,
		s:    s,
		zeta: zeta,
	}
}

// Next generates the next Zipfian-distributed value.
func (z *ZipfGenerator) Next() int {
	if z.n <= 0 {
		return 0
	}

	// Generate random value between 0 and 1
	randVal := z.randomFloat()

	// Find the value using inverse transform sampling
	cumulative := 0.0
	for i := 1; i <= z.n; i++ {
		probability := (1.0 / math.Pow(float64(i), z.s)) / z.zeta
		cumulative += probability

		if randVal <= cumulative {
			return i - 1 // Convert to 0-based index
		}
	}

	// Fallback (shouldn't happen with proper math)
	return z.n - 1
}

// randomFloat generates a random float between 0 and 1.
func (z *ZipfGenerator) randomFloat() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	return float64(n.Int64()) / 1000000.0
}

// UniformGenerator generates values following a uniform distribution.
type UniformGenerator struct {
	min int
	max int
}

// NewUniformGenerator creates a new uniform generator.
func NewUniformGenerator(minVal, maxVal int) *UniformGenerator {
	if minVal >= maxVal {
		maxVal = minVal + 1
	}
	return &UniformGenerator{min: minVal, max: maxVal}
}

// Next generates the next uniformly distributed value.
func (u *UniformGenerator) Next() int {
	if u.min >= u.max {
		return u.min
	}

	n, _ := rand.Int(rand.Reader, big.NewInt(int64(u.max-u.min)))
	return u.min + int(n.Int64())
}

// ExponentialGenerator generates values following an exponential distribution.
// This is useful for generating realistic latency values.
type ExponentialGenerator struct {
	lambda float64 // rate parameter (1/mean)
}

// NewExponentialGenerator creates a new exponential generator
// mean: the mean value of the distribution
func NewExponentialGenerator(mean float64) *ExponentialGenerator {
	if mean <= 0 {
		mean = 1.0
	}
	return &ExponentialGenerator{lambda: 1.0 / mean}
}

// Next generates the next exponentially distributed value.
func (e *ExponentialGenerator) Next() float64 {
	randVal := e.randomFloat()
	if randVal >= 1.0 {
		randVal = 0.999999 // Avoid log(0)
	}
	return -math.Log(1.0-randVal) / e.lambda
}

// randomFloat generates a random float between 0 and 1.
func (e *ExponentialGenerator) randomFloat() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	return float64(n.Int64()) / 1000000.0
}

// NormalGenerator generates values following a normal (Gaussian) distribution.
type NormalGenerator struct {
	mean    float64
	stddev  float64
	hasNext bool
	nextVal float64
}

// NewNormalGenerator creates a new normal generator.
func NewNormalGenerator(mean, stddev float64) *NormalGenerator {
	return &NormalGenerator{
		mean:    mean,
		stddev:  stddev,
		hasNext: false,
	}
}

// Next generates the next normally distributed value using Box-Muller transform.
func (n *NormalGenerator) Next() float64 {
	if n.hasNext {
		n.hasNext = false
		return n.nextVal
	}

	// Generate two independent uniform random variables
	u1 := n.randomFloat()
	u2 := n.randomFloat()

	// Box-Muller transform
	z0 := math.Sqrt(-2.0*math.Log(u1)) * math.Cos(2.0*math.Pi*u2)
	z1 := math.Sqrt(-2.0*math.Log(u1)) * math.Sin(2.0*math.Pi*u2)

	// Store the second value for next call
	n.nextVal = n.mean + n.stddev*z1
	n.hasNext = true

	return n.mean + n.stddev*z0
}

// randomFloat generates a random float between 0 and 1.
func (n *NormalGenerator) randomFloat() float64 {
	randVal, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	return float64(randVal.Int64()) / 1000000.0
}
