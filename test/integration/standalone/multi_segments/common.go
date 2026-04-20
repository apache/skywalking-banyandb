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

// Package multisegments provides shared test setup for multi-segment integration tests.
package multisegments

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	casesmeasure "github.com/apache/skywalking-banyandb/test/cases/measure"
	casesstream "github.com/apache/skywalking-banyandb/test/cases/stream"
	casestopn "github.com/apache/skywalking-banyandb/test/cases/topn"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace"
)

var (
	result     setupResult
	connection *grpc.ClientConn
	goods      []gleak.Goroutine
)

type setupResult struct {
	restart  func() (string, func())
	now      time.Time
	baseTime time.Time
	stopFunc func()
	addr     string
}

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	path, diskCleanupFn, pathErr := test.NewSpace()
	gomega.Expect(pathErr).NotTo(gomega.HaveOccurred())
	ports, portsErr := test.AllocateFreePorts(5)
	gomega.Expect(portsErr).NotTo(gomega.HaveOccurred())
	addr, _, closeFunc := setup.ClosableStandalone(config, path, ports)
	ns := timestamp.NowMilli().UnixNano()
	now := time.Unix(0, ns-ns%int64(time.Minute))
	baseTime := time.Date(now.Year(), now.Month(), now.Day(), 0o0, 0o2, 0, 0, now.Location())
	test_cases.Initialize(addr, baseTime)
	prevClose, currClose := closeFunc, func() {}
	result = setupResult{
		addr:     addr,
		now:      now,
		baseTime: baseTime,
		restart: func() (string, func()) {
			time.Sleep(5 * time.Second)
			prevClose()
			time.Sleep(3 * time.Second)
			currClose()
			addr, _, closeFunc := setup.EmptyClosableStandalone(config, path, ports)
			prevClose, currClose = currClose, closeFunc
			return addr, closeFunc
		},
		stopFunc: func() {
			currClose()
			prevClose()
			diskCleanupFn()
			tmpDirCleanup()
		},
	}
	return []byte(result.addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesstream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.baseTime,
	}
	casesmeasure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.baseTime,
	}
	casestopn.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.baseTime,
	}
	casestrace.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   result.baseTime,
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if connection != nil {
		gomega.Expect(connection.Close()).To(gomega.Succeed())
	}
	if round2Conn != nil {
		gomega.Expect(round2Conn.Close()).To(gomega.Succeed())
	}
}, func() {})

var _ = ginkgo.ReportAfterSuite("Integration Query Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		if result.stopFunc != nil {
			result.stopFunc()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})
