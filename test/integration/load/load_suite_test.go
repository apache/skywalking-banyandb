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

package integration_load_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases_stream_data "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

func TestIntegrationLoad(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Load Suite", Label("integration", "slow"))
}

var _ = Describe("Load Test Suit", func() {
	var (
		connection *grpc.ClientConn
		now        time.Time
		deferFunc  func()
		goods      []gleak.Goroutine
		addr       string
	)

	BeforeEach(func() {
		Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).Should(Succeed())
		goods = gleak.Goroutines()
		addr, _, deferFunc = setup.Standalone()
		Eventually(
			helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(Succeed())
		var err error
		connection, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		days := 7
		hours := 24
		minutes := 60
		interval := 10 * time.Second
		c := time.Now()
		for i := 0; i < days; i++ {
			date := c.Add(-time.Hour * time.Duration((days-i)*24))
			for h := 0; h < hours; h++ {
				hour := date.Add(time.Hour * time.Duration(h))
				start := time.Now()
				for j := 0; j < minutes; j++ {
					n := hour.Add(time.Minute * time.Duration(j))
					ns := n.UnixNano()
					now = time.Unix(0, ns-ns%int64(time.Minute))
					// stream
					cases_stream_data.Write(connection, "data.json", now, interval)
				}
				logger.Infof("written stream in %s took %s \n", hour, time.Since(start))
			}
		}
		Expect(connection.Close()).To(Succeed())
	})

	It("should read data", func() {
		var err error
		connection, err = grpchelper.Conn(addr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		sharedContext := helpers.SharedContext{
			Connection: connection,
			BaseTime:   now,
		}
		query := &streamv1.QueryRequest{
			Metadata: &commonv1.Metadata{
				Name:  "sw",
				Group: "default",
			},
			Projection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "searchable",
						Tags: []string{"trace_id"},
					},
				},
			},
		}
		query.TimeRange = helpers.TimeRange(helpers.Args{Input: "all", Duration: 1 * time.Hour}, sharedContext)
		c := streamv1.NewStreamServiceClient(sharedContext.Connection)
		ctx := context.Background()
		resp, err := c.Query(ctx, query)
		Expect(err).NotTo(HaveOccurred())
		GinkgoWriter.Printf("query result: %s elements\n", resp.GetElements())
		Expect(len(resp.GetElements())).To(BeNumerically(">", 0))
	})

	AfterEach(func() {
		if connection != nil {
			Expect(connection.Close()).To(Succeed())
		}
		deferFunc()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})
})
