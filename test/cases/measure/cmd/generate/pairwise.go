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

package main

// Pair holds a parameter-value pair for pairwise testing.
type Pair struct {
	Param string
	Value string
}

// TestVector is a set of parameter-value assignments.
type TestVector map[string]string

// PairwiseGenerate generates a minimal set of test vectors covering all pairs
// of parameter values using a simplified IPO (In-Parameter-Order) algorithm.
func PairwiseGenerate(params map[string][]string, constraints []ConstraintFunc) []TestVector {
	paramNames := sortedParamNames(params)
	if len(paramNames) == 0 {
		return nil
	}

	// Collect all required pairs
	requiredPairs := collectAllPairs(params, constraints)

	// Build test vectors using greedy pair covering
	var vectors []TestVector
	coveredPairs := make(map[Pair]bool)

	for len(coveredPairs) < len(requiredPairs) {
		tv := buildVector(params, paramNames, coveredPairs, constraints)
		if tv == nil {
			break
		}
		// Mark newly covered pairs
		newCovers := 0
		for _, pn := range paramNames {
			for _, pn2 := range paramNames {
				if pn >= pn2 {
					continue
				}
				p := Pair{Param: pn + "=" + tv[pn], Value: pn2 + "=" + tv[pn2]}
				if !coveredPairs[p] {
					coveredPairs[p] = true
					newCovers++
				}
			}
		}
		if newCovers == 0 {
			break
		}
		vectors = append(vectors, tv)
	}
	return vectors
}

// ConstraintFunc returns false if the combination is invalid.
type ConstraintFunc func(tv TestVector) bool

func collectAllPairs(params map[string][]string, constraints []ConstraintFunc) map[Pair]bool {
	pairs := make(map[Pair]bool)
	paramNames := sortedParamNames(params)
	for _, pn := range paramNames {
		for _, pn2 := range paramNames {
			if pn >= pn2 {
				continue
			}
			for _, v1 := range params[pn] {
				for _, v2 := range params[pn2] {
					tv := TestVector{pn: v1, pn2: v2}
					if validConstraints(tv, constraints) {
						pairs[Pair{Param: pn + "=" + v1, Value: pn2 + "=" + v2}] = true
					}
				}
			}
		}
	}
	return pairs
}

func buildVector(params map[string][]string, paramNames []string, coveredPairs map[Pair]bool, constraints []ConstraintFunc) TestVector {
	tv := make(TestVector)
	fillVector(tv, params, paramNames, 0, coveredPairs, constraints)
	return tv
}

func fillVector(tv TestVector, params map[string][]string, paramNames []string, paramIdx int, coveredPairs map[Pair]bool, constraints []ConstraintFunc) bool {
	if paramIdx >= len(paramNames) {
		return validConstraints(tv, constraints)
	}
	pn := paramNames[paramIdx]
	bestVal := ""
	bestScore := -1
	for _, val := range params[pn] {
		tv[pn] = val
		if !validConstraints(tv, constraints) {
			continue
		}
		score := countNewPairs(tv, paramNames, paramIdx, coveredPairs)
		if score > bestScore {
			bestScore = score
			bestVal = val
		}
	}
	if bestVal == "" {
		// Fallback: pick first valid value
		for _, val := range params[pn] {
			tv[pn] = val
			if validConstraints(tv, constraints) {
				bestVal = val
				break
			}
		}
		if bestVal == "" {
			tv[pn] = params[pn][0]
		}
	}
	tv[pn] = bestVal
	return fillVector(tv, params, paramNames, paramIdx+1, coveredPairs, constraints)
}

func countNewPairs(tv TestVector, paramNames []string, currentIdx int, coveredPairs map[Pair]bool) int {
	count := 0
	pn := paramNames[currentIdx]
	for prevIdx := 0; prevIdx < currentIdx; prevIdx++ {
		prevPn := paramNames[prevIdx]
		p := Pair{Param: prevPn + "=" + tv[prevPn], Value: pn + "=" + tv[pn]}
		if !coveredPairs[p] {
			count++
		}
	}
	return count
}

func validConstraints(tv TestVector, constraints []ConstraintFunc) bool {
	for _, constraint := range constraints {
		if !constraint(tv) {
			return false
		}
	}
	return true
}

func sortedParamNames(params map[string][]string) []string {
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	// Deterministic order
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[i] > names[j] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}
