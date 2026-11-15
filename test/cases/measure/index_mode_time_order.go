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

package measure_test

import (
	"context"
	"fmt"
	"strings"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// The index-mode measure service_traffic holds four rows: ids 1, 2 and 3 one minute apart, id 3 the newest, and
// id 4 two days earlier, in another segment. A query without ORDER BY returns them in index order, which is not
// specified; an explicit ORDER BY must order them by timestamp, before LIMIT applies, within a segment and across
// segments, the way it does on a measure that is not index mode (apache/skywalking#14218).
var _ = g.DescribeTable("Index mode time order", func(query string, wantIDs ...string) {
	gm.Eventually(func(innerGm gm.Gomega) {
		client := bydbqlv1.NewBydbQLServiceClient(SharedContext.Connection)
		resp, err := client.Query(context.Background(), &bydbqlv1.QueryRequest{Query: query})
		innerGm.Expect(err).NotTo(gm.HaveOccurred(), query)
		points := resp.GetMeasureResult().GetDataPoints()
		var ids []string
		for _, point := range points {
			for _, family := range point.GetTagFamilies() {
				for _, tag := range family.GetTags() {
					if tag.GetKey() == "id" {
						ids = append(ids, tag.GetValue().GetStr().GetValue())
					}
				}
			}
		}
		innerGm.Expect(ids).To(gm.Equal(wantIDs), fmt.Sprintf("%s\nrows by timestamp: %v", query, func() []string {
			var stamps []string
			for _, point := range points {
				stamps = append(stamps, point.GetTimestamp().AsTime().Format("01-02T15:04"))
			}
			return stamps
		}()))
	}, flags.EventuallyTimeout).Should(gm.Succeed())
},
	g.Entry("one segment, ORDER BY DESC returns the newest first",
		indexModeTimeQuery("TIME > '-15m' ORDER BY DESC"), "3", "2", "1"),
	g.Entry("one segment, ORDER BY DESC LIMIT 2 returns the two newest",
		indexModeTimeQuery("TIME > '-15m' ORDER BY DESC LIMIT 2"), "3", "2"),
	g.Entry("one segment, ORDER BY ASC returns the oldest first",
		indexModeTimeQuery("TIME > '-15m' ORDER BY ASC"), "1", "2", "3"),
	g.Entry("one segment, ORDER BY ASC LIMIT 2 returns the two oldest",
		indexModeTimeQuery("TIME > '-15m' ORDER BY ASC LIMIT 2"), "1", "2"),
	g.Entry("one segment, ORDER BY TIME DESC is the same order",
		indexModeTimeQuery("TIME > '-15m' ORDER BY TIME DESC"), "3", "2", "1"),
	g.Entry("two segments, ORDER BY DESC puts the older segment last",
		indexModeTimeQuery("TIME > '-3d' ORDER BY DESC"), "3", "2", "1", "4"),
	g.Entry("two segments, ORDER BY DESC LIMIT 2 returns the two newest",
		indexModeTimeQuery("TIME > '-3d' ORDER BY DESC LIMIT 2"), "3", "2"),
	g.Entry("two segments, ORDER BY ASC puts the older segment first",
		indexModeTimeQuery("TIME > '-3d' ORDER BY ASC"), "4", "1", "2", "3"),
	g.Entry("two segments, ORDER BY ASC LIMIT 2 returns the two oldest",
		indexModeTimeQuery("TIME > '-3d' ORDER BY ASC LIMIT 2"), "4", "1"),
	g.Entry("two segments, ORDER BY TIME DESC is the same order",
		indexModeTimeQuery("TIME > '-3d' ORDER BY TIME DESC"), "3", "2", "1", "4"),
	g.Entry("one segment, ORDER BY DESC LIMIT 1 returns the newest",
		indexModeTimeQuery("TIME > '-15m' ORDER BY DESC LIMIT 1"), "3"),
	g.Entry("one segment, ORDER BY ASC LIMIT 1 returns the oldest",
		indexModeTimeQuery("TIME > '-15m' ORDER BY ASC LIMIT 1"), "1"),
	g.Entry("two segments, ORDER BY DESC LIMIT 1 returns the newest",
		indexModeTimeQuery("TIME > '-3d' ORDER BY DESC LIMIT 1"), "3"),
	g.Entry("two segments, ORDER BY ASC LIMIT 1 returns the oldest",
		indexModeTimeQuery("TIME > '-3d' ORDER BY ASC LIMIT 1"), "4"),
)

func indexModeTimeQuery(tail string) string {
	return "SELECT id, name FROM MEASURE service_traffic IN index_mode " + strings.TrimSpace(tail)
}
