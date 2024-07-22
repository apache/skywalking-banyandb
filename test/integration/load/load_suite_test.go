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
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/montanaflynn/stats"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases_stream_data "github.com/apache/skywalking-banyandb/test/cases/stream/data"
)

func TestIntegrationLoad(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Load Suite", Label("integration", "slow"))
}

const (
	minutes        = 10 * 24 * 60
	interval       = 10 * time.Second
	queryInterval  = time.Hour
	reportInterval = 24 * time.Hour
)

var _ = Describe("Load Test Suit", func() {
	var (
		connection *grpc.ClientConn
		deferFunc  func()
		goods      []gleak.Goroutine
		dir        string
	)

	queryFn := func(now time.Time, dur time.Duration) time.Duration {
		query := &streamv1.QueryRequest{
			Name:   "sw",
			Groups: []string{"default"},
			Projection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{
						Name: "searchable",
						Tags: []string{"trace_id"},
					},
				},
			},
		}
		query.TimeRange = helpers.TimeRange(helpers.Args{Input: "all", Duration: dur}, helpers.SharedContext{
			Connection: connection,
			BaseTime:   now,
		})
		c := streamv1.NewStreamServiceClient(connection)
		ctx := context.Background()
		GinkgoWriter.Printf("querying at %s\n", now)
		start := time.Now()
		resp, err := c.Query(ctx, query)
		Expect(err).NotTo(HaveOccurred())
		latency := time.Since(start)
		size := len(resp.GetElements())
		Expect(size).Should(BeNumerically(">", 0))
		GinkgoWriter.Printf("query result: %s elements using %s \n", size, latency)
		return latency
	}

	BeforeEach(func() {
		Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: "debug",
		})).Should(Succeed())
		test.Cleanup()
		goods = gleak.Goroutines()
		var addr string
		addr, _, deferFunc = setup.Standalone()
		Eventually(
			helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(Succeed())
		var err error
		connection, err = grpchelper.Conn(addr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		files, err := os.ReadDir(os.TempDir())
		Expect(err).NotTo(HaveOccurred())

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "banyandb-") {
				dir = path.Join(os.TempDir(), file.Name())
				break
			}
		}
		Expect(dir).NotTo(BeEmpty())
	})

	It("should pass without any error after running 10 days with 1 day segment and 3 days ttl", func() {
		ns := time.Now().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		var lastQueryTime, lastReportTime time.Time
		latest1HourQueryLatencyData := make([]float64, 0)
		allQueryLatencyData := make([]float64, 0)
		for i := 0; i < minutes; i++ {
			GinkgoWriter.Printf("writing data at %s\n", now)
			cases_stream_data.Write(connection, "sw", now, interval)
			if now.Sub(lastQueryTime) > queryInterval {
				latency := queryFn(now, time.Hour)
				latest1HourQueryLatencyData = append(latest1HourQueryLatencyData, float64(latency.Milliseconds()))
				latency = queryFn(now, 11*24*time.Hour)
				allQueryLatencyData = append(allQueryLatencyData, float64(latency.Milliseconds()))
				lastQueryTime = now
			}
			if now.Sub(lastReportTime) > reportInterval {
				logger.Infof("reporting at %s\n", now)
				// Print files in the directory with prefix "banyandb-" in "tmp" directory
				helpers.PrintDiskUsage(dir, 4, 0)
				analysis("latest 1 hour query latency", latest1HourQueryLatencyData)
				analysis("all query latency", allQueryLatencyData)
				lastReportTime = now
				latest1HourQueryLatencyData = latest1HourQueryLatencyData[:0]
				allQueryLatencyData = allQueryLatencyData[:0]
			}
			now = now.Add(time.Minute)
		}
	})

	AfterEach(func() {
		if connection != nil {
			Expect(connection.Close()).To(Succeed())
		}
		deferFunc()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})
})

func analysis(name string, data []float64) {
	min, _ := stats.Min(data)
	max, _ := stats.Max(data)
	mean, _ := stats.Mean(data)
	median, _ := stats.Median(data)
	p90, _ := stats.Percentile(data, 90)
	p95, _ := stats.Percentile(data, 95)
	p98, _ := stats.Percentile(data, 98)
	p99, _ := stats.Percentile(data, 99)
	logger.Infof("%s: min: %f, max: %f, mean: %f, median: %f, p90: %f, p95: %f, p98: %f, p99: %f\n", name, min, max, mean, median, p90, p95, p98, p99)
}
