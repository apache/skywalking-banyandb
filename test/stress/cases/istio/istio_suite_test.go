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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestIstio(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Istio Suite", Label("integration", "slow"))
}

var _ = Describe("Istio", func() {
	BeforeEach(func() {
		Expect(logger.Init(logger.Logging{
			Env:   "dev",
			Level: "info",
		})).To(Succeed())
	})
	It("should pass", func() {
		addr, _, deferFunc := setup.CommonWithSchemaLoaders([]setup.SchemaLoader{&preloadService{name: "oap"}})
		DeferCleanup(deferFunc)
		Eventually(helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials())),
			flags.EventuallyTimeout).Should(Succeed())
		conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			conn.Close()
		})
		Expect(ReadAndWriteFromFile(extractData(), conn)).To(Succeed())
	})
})

func ReadAndWriteFromFile(filePath string, conn *grpc.ClientConn) error {
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
		return fmt.Errorf("failed to create write client: %w", err)
	}
	flush := func(createClient bool) error {
		if errClose := client.CloseSend(); errClose != nil {
			return fmt.Errorf("failed to close send: %w", errClose)
		}
		bulkSize = 2000
		_, err = client.Recv()
		if err != nil && errors.Is(err, io.EOF) {
			return fmt.Errorf("failed to receive client: %w", err)
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
				return fmt.Errorf("failed to read line from file: %w", errRead)
			}
			if errRead != nil && errRead.Error() == "EOF" {
				break
			}
			var req measurev1.WriteRequest
			if errUnmarshal := protojson.Unmarshal([]byte(jsonMsg), &req); errUnmarshal != nil {
				return fmt.Errorf("failed to unmarshal JSON message: %w", errUnmarshal)
			}

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
			return err
		}
	}
	return flush(false)
}
