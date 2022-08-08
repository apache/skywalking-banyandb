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
package tsdb_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Series", func() {
	Context("TimeRange", func() {
		Context("Contains", func() {
			verifyFn := func(start, end string, includeStart, includeEnd bool, ts string, expected bool) {
				startTime, _ := time.Parse("20060202", start)
				endTime, _ := time.Parse("20060202", end)
				tsTime, _ := time.Parse("20060202", ts)
				Expect(timestamp.NewTimeRange(startTime, endTime, includeStart, includeEnd).Contains(uint64(tsTime.UnixNano()))).To(Equal(expected))
			}
			DescribeTable("It's a exclusive range",
				func(start, end, ts string, expected bool) {
					verifyFn(start, end, false, false, ts, expected)
				},
				Entry("is in the middle", "20220205", "20220107", "20220106", true),
				Entry("is at the lower", "20220205", "20220107", "20220105", false),
				Entry("is at the upper", "20220205", "20220107", "20220107", false),
				Entry("is before the lower", "20220205", "20220107", "20220104", false),
				Entry("is after the upper", "20220205", "20220107", "20220108", false),
			)
			DescribeTable("It's a inclusive range",
				func(start, end, ts string, expected bool) {
					verifyFn(start, end, true, true, ts, expected)
				},
				Entry("is in the middle", "20220205", "20220107", "20220106", true),
				Entry("is at the lower", "20220205", "20220107", "20220105", true),
				Entry("is at the upper", "20220205", "20220107", "20220107", true),
				Entry("is before the lower", "20220205", "20220107", "20220104", false),
				Entry("is after the upper", "20220205", "20220107", "20220108", false),
			)
			DescribeTable("It's a inclusive lower and exclusive upper range",
				func(start, end, ts string, expected bool) {
					verifyFn(start, end, true, false, ts, expected)
				},
				Entry("is in the middle", "20220205", "20220107", "20220106", true),
				Entry("is at the lower", "20220205", "20220107", "20220105", true),
				Entry("is at the upper", "20220205", "20220107", "20220107", false),
				Entry("is before the lower", "20220205", "20220107", "20220104", false),
				Entry("is after the upper", "20220205", "20220107", "20220108", false),
			)
			DescribeTable("It's a exclusive lower and inclusive upper range",
				func(start, end, ts string, expected bool) {
					verifyFn(start, end, false, true, ts, expected)
				},
				Entry("is in the middle", "20220205", "20220107", "20220106", true),
				Entry("is at the lower", "20220205", "20220107", "20220105", false),
				Entry("is at the upper", "20220205", "20220107", "20220107", true),
				Entry("is before the lower", "20220205", "20220107", "20220104", false),
				Entry("is after the upper", "20220205", "20220107", "20220108", false),
			)
		})
		Context("Overlapping", func() {
			verifyFn := func(start1, end1, start2, end2 string, expected bool) {
				startTime1, _ := time.Parse("20060102", start1)
				endTime1, _ := time.Parse("20060102", end1)
				startTime2, _ := time.Parse("20060102", start2)
				endTime2, _ := time.Parse("20060102", end2)
				includes := []bool{true, false}

				for _, r1l := range includes {
					for _, r1u := range includes {
						for _, r2l := range includes {
							for _, r2u := range includes {
								By(fmt.Sprintf("r1 lower:%v upper:%v. r1 lower:%v upper:%v", r1l, r1u, r2l, r2u), func() {
									r1 := timestamp.NewTimeRange(startTime1, endTime1, r1l, r1u)
									r2 := timestamp.NewTimeRange(startTime2, endTime2, r2l, r2u)
									Expect(r1.Overlapping(r2)).To(Equal(expected))
									Expect(r2.Overlapping(r1)).To(Equal(expected))
								})
							}
						}
					}
				}
			}
			DescribeTable("Each range type's behavior is identical",
				verifyFn,
				Entry("is no overlapping", "20220205", "20220107", "20220108", "20220112", false),
				Entry("is the two range are identical", "20220105", "20220107", "20220105", "20220107", true),
				Entry("is the one includes the other", "20220102", "20220107", "20220103", "20220106", true),
				Entry("is the one includes the other, the upper bounds are identical", "20220102", "20220107", "20220103", "20220107", true),
				Entry("is the one includes the other, the lower bounds are identical", "20220102", "20220107", "20220102", "20220106", true),
				Entry("is they have an intersection", "20220102", "20220105", "20220103", "20220106", true),
			)
			adjacentVerifyFn := func(include1, include2, expected bool) {
				startTime1, _ := time.Parse("20060102", "20210105")
				endTime1, _ := time.Parse("20060102", "20210107")
				startTime2, _ := time.Parse("20060102", "20210107")
				endTime2, _ := time.Parse("20060102", "20210109")
				r1 := timestamp.NewTimeRange(startTime1, endTime1, false, include1)
				r2 := timestamp.NewTimeRange(startTime2, endTime2, include2, false)
				Expect(r1.Overlapping(r2)).To(Equal(expected))
			}

			DescribeTable("They are adjacent",
				adjacentVerifyFn,
				Entry("is they are inclusive", true, true, true),
				Entry("is that range1 includes upper, but range2 excludes lower", true, false, false),
				Entry("is that range1 excludes upper, but range2 includes lower", false, true, false),
				Entry("is they are exclusive", false, false, false),
			)
		})
	})
})
