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

// coverPairs deterministically selects a subset of combos (each a tuple of dimension
// value indexes) such that every value of every dimension and every value pair of the
// dimension pairs listed in pairs appears in at least one selected combo. A nil pairs
// requires every pair of dimensions. The selection is greedy and ordered, so equal
// input always yields equal output; the race build of the differential tests runs it
// instead of the full cross product. A non-nil cost ranks combos by how expensive they
// are to run (at least 1): the greedy step takes the best gain per unit of cost, so
// the cheap combos that cover a value are preferred over the heavy ones.
func coverPairs(combos [][]int, pairs [][2]int, cost func(combo []int) int) [][]int {
	if len(combos) == 0 {
		return nil
	}
	dims := len(combos[0])
	if pairs == nil {
		for i := range dims {
			for j := i + 1; j < dims; j++ {
				pairs = append(pairs, [2]int{i, j})
			}
		}
	}
	type item struct{ a, b, av, bv int }
	items := func(c []int) []item {
		out := make([]item, 0, dims+len(pairs))
		for d := range dims {
			out = append(out, item{d, -1, c[d], -1})
		}
		for _, p := range pairs {
			out = append(out, item{p[0], p[1], c[p[0]], c[p[1]]})
		}
		return out
	}
	missing := map[item]struct{}{}
	for _, c := range combos {
		for _, it := range items(c) {
			missing[it] = struct{}{}
		}
	}
	var picked [][]int
	used := make([]bool, len(combos))
	for len(missing) > 0 {
		best, bestScore := -1, 0.0
		for i, c := range combos {
			if used[i] {
				continue
			}
			gain := 0
			for _, it := range items(c) {
				if _, ok := missing[it]; ok {
					gain++
				}
			}
			weight := 1
			if cost != nil {
				weight = max(1, cost(c))
			}
			if score := float64(gain) / float64(weight); score > bestScore {
				best, bestScore = i, score
			}
		}
		used[best] = true
		picked = append(picked, combos[best])
		for _, it := range items(combos[best]) {
			delete(missing, it)
		}
	}
	return picked
}
