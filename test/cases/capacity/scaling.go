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

package capacity

import (
	"fmt"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

const (
	// minScaling and maxScaling bound how much the capacity may grow when the query memory doubles. Fixed costs make it a little
	// more than double, and the storage state of the data a little less or more.
	minScaling = 1.5
	maxScaling = 3.0
	// marginalMargin is how many times the memory a query really holds for each result it returns may exceed what the budget
	// charges for each: the budget is an estimate that has to cover the real memory, and a client in the same heap holds
	// copies of the response the budget does not see.
	marginalMargin = 2.0
	// minSlopeSignal is the smallest growth of the heap between the two limits of the measurement that is trusted: under it the
	// difference is dominated by the noise of the garbage collector.
	minSlopeSignal = 1 << 20
	// slopeLowDivisor places the lower limit of the measurement at an eighth of the upper one.
	slopeLowDivisor = 8
)

// CheckScaling asserts the property the memory budget exists for: a node with twice the query memory serves a larger limit,
// by about twice, whatever the data set holds. It compares two runs of the same scenario in the same process, so the
// constants of the machine cancel.
func CheckScaling(small, large *Result, scenario string) {
	ratio := float64(large.MaxLimit) / float64(small.MaxLimit)
	report := fmt.Sprintf("%s: the capacity goes from %d to %d results when the query memory doubles (x%.2f)\n", scenario, small.MaxLimit, large.MaxLimit, ratio)
	g.AddReportEntry("scaling "+scenario, report)
	fmt.Print(report)
	gm.Expect(ratio).To(gm.BeNumerically(">=", minScaling), "doubling the query memory must raise the capacity: %s", report)
	gm.Expect(ratio).To(gm.BeNumerically("<=", maxScaling), "doubling the query memory must not raise the capacity more than threefold: %s", report)
}

// CheckMarginalMemory compares what each further result costs in real memory with what the budget charges for it, on the
// node with the larger memory. The budget's cost is the extra allowance that doubling the pool gives, over the extra results
// it buys. The real cost is the growth of the peak heap between a low and a high limit over the results between them. Both are
// differences, so the fixed costs of a query, which depend on the machine, cancel in each. The spec fails when a result
// costs in memory more than marginalMargin times what the budget charges for it: the budget would then admit queries that
// hold more than it says.
func CheckMarginalMemory(small, large *Result, smallPool, largePool uint64, probe func(int) (Outcome, error), scenario string) {
	budgetPerResult := float64(queryAllowance(largePool)-queryAllowance(smallPool)) / float64(large.MaxLimit-small.MaxLimit)
	high := large.MaxLimit - large.MaxLimit/10
	low := max(1, high/slopeLowDivisor)
	heapAt := func(limit int) uint64 {
		return MedianHeap(func() {
			_, err := probe(limit)
			gm.Expect(err).NotTo(gm.HaveOccurred(), "limit %d, below the capacity, must be served while its memory is measured", limit)
		})
	}
	heapLow, heapHigh := heapAt(low), heapAt(high)
	report := fmt.Sprintf("%s: the budget charges %.0f bytes for each further result; the heap grew %d bytes at limit %d and %d bytes at limit %d\n",
		scenario, budgetPerResult, heapLow, low, heapHigh, high)
	if heapHigh <= heapLow+minSlopeSignal {
		report += "  the growth between the limits is within the noise of the measurement, so it is not compared\n"
		g.AddReportEntry("marginal memory "+scenario, report)
		fmt.Print(report)
		return
	}
	realPerResult := float64(heapHigh-heapLow) / float64(high-low)
	report += fmt.Sprintf("  a further result holds %.0f bytes of heap, %.2f times what the budget charges\n", realPerResult, realPerResult/budgetPerResult)
	g.AddReportEntry("marginal memory "+scenario, report)
	fmt.Print(report)
	gm.Expect(realPerResult).To(gm.BeNumerically("<=", marginalMargin*budgetPerResult),
		"a further result holds more memory than the budget charges for it\n%s", report)
}
