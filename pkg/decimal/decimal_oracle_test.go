// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decimal_test

import (
	"errors"
	"math/big"
	"math/rand/v2"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/decimal"
)

// Compare bounded arithmetic with an independent arbitrary-precision oracle.
func TestInt128Oracle(t *testing.T) {
	random := rand.New(rand.NewPCG(37, 91)) //nolint:gosec // Deterministic arithmetic test data, not security-sensitive randomness.
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	limit.Sub(limit, big.NewInt(1))
	for trial := 0; trial < 128; trial++ {
		coefficient := decimal.FromInt64(random.Int64())
		expected := big.NewInt(0)
		expected.SetString(coefficient.String(0), 10)
		if trial%2 == 0 {
			coefficient = decimal.FromInt64(-random.Int64())
			expected.SetString(coefficient.String(0), 10)
		}
		for step := 0; step < 110; step++ {
			operand := coefficient
			oracleOperand := new(big.Int).Set(expected)
			if step%3 == 0 {
				v := int64(random.Uint64())
				operand = decimal.FromInt64(v)
				oracleOperand.SetInt64(v)
			}
			sum := new(big.Int).Add(expected, oracleOperand)
			actual, err := coefficient.Add(operand)
			magnitude := new(big.Int).Abs(sum)
			if magnitude.Cmp(limit) > 0 {
				if !errors.Is(err, decimal.ErrOverflow) {
					t.Fatalf("trial %d step %d: expected overflow, got %v", trial, step, err)
				}
				break
			}
			if err != nil || actual.String(0) != sum.String() {
				t.Fatalf("trial %d step %d: got %s (%v), want %s", trial, step, actual.String(0), err, sum)
			}
			scale, otherScale := uint8(random.IntN(19)), uint8(random.IntN(19))
			left := new(big.Int).Mul(sum, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(otherScale)), nil))
			right := new(big.Int).Mul(oracleOperand, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
			if got, want := actual.CmpScaled(scale, operand, otherScale), left.Cmp(right); got != want {
				t.Fatalf("trial %d step %d: scaled comparison got %d, want %d", trial, step, got, want)
			}
			divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
			formatted := new(big.Rat).SetFrac(sum, divisor).FloatString(int(scale))
			if got := actual.String(scale); got != formatted {
				t.Fatalf("trial %d step %d: format got %s, want %s", trial, step, got, formatted)
			}
			coefficient, expected = actual, sum
		}
	}
}
