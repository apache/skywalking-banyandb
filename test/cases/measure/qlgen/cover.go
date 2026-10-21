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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
)

// Vector is one complete assignment of generator dimensions.
type Vector map[string]string

// PairCoverage records all feasible pair obligations and their selected witnesses.
type PairCoverage struct {
	Dimensions map[string][]string `json:"dimensions"`
	Uncovered  []string            `json:"uncovered"`
	Required   int                 `json:"required"`
	Covered    int                 `json:"covered"`
	Candidates int                 `json:"candidates"`
	Selected   int                 `json:"selected"`
}

// Cover selects a deterministic cover of every pair occurring in a valid complete
// assignment. Impossible partial combinations never become coverage obligations.
func Cover(dimensions map[string][]string, valid func(Vector) bool) ([]Vector, PairCoverage, error) {
	report := PairCoverage{Dimensions: dimensions, Uncovered: []string{}}
	keys := slices.Sorted(maps.Keys(dimensions))
	if len(keys) < 2 {
		return nil, report, fmt.Errorf("pair generation requires at least two dimensions")
	}
	combinations := 1
	for _, key := range keys {
		if len(dimensions[key]) == 0 || combinations > 25000/len(dimensions[key]) {
			return nil, report, fmt.Errorf("QL generation domain is empty or exceeds 25000 assignments")
		}
		combinations *= len(dimensions[key])
	}
	var candidates []Vector
	var enumerate func(int, Vector)
	enumerate = func(index int, vector Vector) {
		if index == len(keys) {
			if valid == nil || valid(vector) {
				candidates = append(candidates, maps.Clone(vector))
			}
			return
		}
		for _, value := range dimensions[keys[index]] {
			vector[keys[index]] = value
			enumerate(index+1, vector)
		}
	}
	enumerate(0, make(Vector, len(keys)))
	if len(candidates) == 0 {
		return nil, report, fmt.Errorf("QL generation constraints admit no complete assignment")
	}
	report.Candidates = len(candidates)
	obligations := make(map[string]bool)
	pairs := make([][]string, len(candidates))
	for index, vector := range candidates {
		pairs[index] = vectorPairs(keys, vector)
		for _, pair := range pairs[index] {
			obligations[pair] = false
		}
	}
	report.Required = len(obligations)
	var selected []Vector
	for report.Covered < report.Required {
		bestIndex, bestScore := -1, 0
		for index, witness := range pairs {
			score := 0
			for _, pair := range witness {
				if !obligations[pair] {
					score++
				}
			}
			if score > bestScore {
				bestIndex, bestScore = index, score
			}
		}
		if bestIndex < 0 {
			return nil, report, fmt.Errorf("QL pair generation cannot cover remaining obligations")
		}
		selected = append(selected, candidates[bestIndex])
		for _, pair := range pairs[bestIndex] {
			if !obligations[pair] {
				obligations[pair] = true
				report.Covered++
			}
		}
	}
	report.Selected = len(selected)
	return selected, report, nil
}

func vectorPairs(keys []string, vector Vector) []string {
	var pairs []string
	for left, key := range keys {
		for _, other := range keys[left+1:] {
			pairs = append(pairs, key+"="+strconv.Quote(vector[key])+"|"+other+"="+strconv.Quote(vector[other]))
		}
	}
	return pairs
}

func validatePairWitnesses(report PairCoverage, cases []Case) error {
	if !maps.EqualFunc(report.Dimensions, interactionDimensions(), slices.Equal[[]string]) {
		return fmt.Errorf("generated QL pair dimensions are stale")
	}
	_, required, err := Cover(report.Dimensions, nil)
	if err != nil {
		return err
	}
	if report.Required != required.Required || report.Candidates != required.Candidates || len(report.Uncovered) != 0 {
		return fmt.Errorf("generated QL pair obligations are stale or incomplete")
	}
	keys := slices.Sorted(maps.Keys(report.Dimensions))
	covered := make(map[string]bool)
	witnesses := 0
	for _, c := range cases {
		if len(c.Dimensions) == 0 {
			continue
		}
		if len(c.Dimensions) != len(keys) || !slices.Contains(c.Features, "combinations.pairwise") || c.WantCode != 0 {
			return fmt.Errorf("invalid QL pair witness %s", c.Name)
		}
		for _, key := range keys {
			if !slices.Contains(report.Dimensions[key], c.Dimensions[key]) {
				return fmt.Errorf("invalid QL pair value in %s: %s", c.Name, key)
			}
		}
		for _, pair := range vectorPairs(keys, c.Dimensions) {
			covered[pair] = true
		}
		witnesses++
	}
	if witnesses != report.Selected || len(covered) != report.Required || report.Covered != len(covered) {
		return fmt.Errorf("generated QL pair coverage is incomplete: got %d of %d pairs", len(covered), report.Required)
	}
	return nil
}
