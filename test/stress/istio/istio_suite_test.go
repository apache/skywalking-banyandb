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

// Package istio provides stress test of the istio scenario.
package istio

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/metric"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestIstio(t *testing.T) {
	gomega.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Istio Suite", g.Label("integration", "slow"))
}

var _ = g.Describe("Istio", func() {
	g.BeforeEach(func() {
		gomega.Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: flags.LogLevel,
		})).To(gomega.Succeed())
	})
	g.It("should pass", func() {
		path, deferFn, err := test.NewSpace()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		measurePath := filepath.Join(path, "measure")
		var ports []int
		ports, err = test.AllocateFreePorts(4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		addr, _, closerServerFunc := setup.ClosableStandaloneWithSchemaLoaders(
			path, ports,
			[]setup.SchemaLoader{&preloadService{name: "oap"}},
			"--logging-level", "info")
		g.DeferCleanup(func() {
			time.Sleep(time.Minute)
			closerServerFunc()
			helpers.PrintDiskUsage(measurePath, 5, 0)
			deferFn()
		})
		gomega.Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(gomega.Succeed())
		bc := &clientCounter{}
		conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(bc))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		closeMetricCollectorCh := metric.Start(measurePath)
		g.DeferCleanup(func() {
			close(closeMetricCollectorCh)
			conn.Close()
		})
		startTime := time.Now()
		writtenCount, err := ReadAndWriteFromFile(extractData(), conn)
		gomega.Expect(err).To(gomega.Succeed())
		endTime := time.Now()

		// Print machine and OS information.
		fmt.Println("Machine and OS information:")
		fmt.Println("  - CPU:", helpers.CPUInfo())
		fmt.Println("  - Memory:", helpers.MemoryInfo())
		fmt.Println("  - OS:", helpers.OSInfo())

		fmt.Printf("written %d items in %s\n", writtenCount, endTime.Sub(startTime).String())
		fmt.Printf("throughput: %f items/s\n", float64(writtenCount)/endTime.Sub(startTime).Seconds())
		fmt.Printf("throughput(kb/s) %f\n", float64(bc.bytesSent.Load())/endTime.Sub(startTime).Seconds()/1024)
		fmt.Printf("latency: %s\n", time.Duration(bc.totalLatency.Load())/time.Duration(writtenCount))
	})
})

func ReadAndWriteFromFile(filePath string, conn *grpc.ClientConn) (int, error) {
	// Open the file for reading

	l := logger.GetLogger("load_test")

	startTime := timestamp.NowMilli()

	var minute, hour, day time.Time
	adjustTime := func(t time.Time) time.Time {
		switch {
		case t == t.Truncate(time.Minute):
			return minute
		case t == t.Truncate(time.Hour):
			return hour
		case t == t.Truncate(24*time.Hour):
			return day
		default:
			panic("invalid time:" + t.String())
		}
	}

	// Read requests from the file and write them using the Write function
	bulkSize := 2000
	c := measurev1.NewMeasureServiceClient(conn)
	ctx := context.Background()
	client, err := c.Write(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to create write client: %w", err)
	}
	writeCount := 0
	flush := func(createClient bool) error {
		if errClose := client.CloseSend(); errClose != nil {
			return fmt.Errorf("failed to close send: %w", errClose)
		}
		bulkSize = 2000
		writeCount += 2000
		for i := 0; i < 2000; i++ {
			_, err = client.Recv()
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("failed to receive client: %w", err)
			}
			if errors.Is(err, io.EOF) {
				break
			}
		}
		if !createClient {
			return nil
		}
		client, err = c.Write(ctx)
		if err != nil {
			return fmt.Errorf("failed to create write client: %w", err)
		}
		return nil
	}
	loop := func(round int) error {
		currentTime := startTime.Add(time.Duration(round) * time.Minute)
		minute = currentTime.Truncate(time.Minute)
		hour = currentTime.Truncate(time.Hour)
		day = currentTime.Truncate(24 * time.Hour)
		for minute.After(time.Now()) {
			l.Info().Msg("sleep 2s...")
			time.Sleep(2 * time.Second)
		}
		l.Info().Time("minute", minute).Msg("start to write")
		file, errOpen := os.Open(filePath)
		if errOpen != nil {
			fmt.Printf("failed to open file: %v\n", errOpen)
			return errOpen
		}
		defer file.Close()
		reader := bufio.NewReader(file)
		for {
			if minute != time.Now().Truncate(time.Minute) {
				break
			}
			jsonMsg, errRead := reader.ReadString('\n')
			if errRead != nil && errRead.Error() != "EOF" {
				return fmt.Errorf("line %d failed to read line from file: %w", 2000-bulkSize, errRead)
			}
			if errRead != nil && errRead.Error() == "EOF" {
				break
			}
			var req measurev1.WriteRequest
			if errUnmarshal := protojson.Unmarshal([]byte(jsonMsg), &req); errUnmarshal != nil {
				return fmt.Errorf("line %d failed to unmarshal JSON message: %w", 2000-bulkSize, errUnmarshal)
			}

			req.MessageId = uint64(time.Now().UnixNano())
			req.DataPoint.Timestamp = timestamppb.New(adjustTime(req.DataPoint.Timestamp.AsTime()))
			// Write the request to the measureService
			if errSend := client.Send(&req); errSend != nil {
				return fmt.Errorf("failed to write request to measureService: %w", errSend)
			}
			bulkSize--
			if bulkSize == 0 {
				l.Info().Msg("flushing 2000 items....")
				if err = flush(true); err != nil {
					return err
				}
			}
			if minute != time.Now().Truncate(time.Minute) {
				l.Info().Msg("write this minute done, flush remaining items....")
				if err = flush(true); err != nil {
					return err
				}
				break
			}
		}
		return nil
	}
	for i := 0; i < 40; i++ {
		if err = loop(i); err != nil {
			return writeCount, err
		}
	}
	return writeCount, flush(false)
}

type clientCounter struct {
	bytesSent    atomic.Int64
	totalLatency atomic.Int64
}

func (*clientCounter) HandleConn(context.Context, stats.ConnStats) {}

func (c *clientCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (c *clientCounter) HandleRPC(_ context.Context, s stats.RPCStats) {
	switch s := s.(type) {
	case *stats.OutPayload:
		c.bytesSent.Add(int64(s.WireLength))
	case *stats.End:
		c.totalLatency.Add(int64(s.EndTime.Sub(s.BeginTime)))
	}
}

func (c *clientCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
