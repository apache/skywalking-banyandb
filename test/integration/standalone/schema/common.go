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

// Package schema contains integration tests for standalone schema deletion.
package schema

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
	casesschema "github.com/apache/skywalking-banyandb/test/cases/schema"
)

var (
	now        time.Time
	stopFunc   func()
	connection *grpc.ClientConn
	goods      []gleak.Goroutine
)

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	gomega.Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(gomega.Succeed())
	pool.EnableStackTracking(true)
	goods = gleak.Goroutines()
	tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
	gomega.Expect(tmpErr).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	addr, _, closeFn := setup.EmptyStandalone(config)
	now = time.Now()
	stopFunc = func() {
		closeFn()
		tmpDirCleanup()
	}
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesschema.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if connection != nil {
		gomega.Expect(connection.Close()).To(gomega.Succeed())
	}
}, func() {})

var _ = ginkgo.ReportAfterSuite("Standalone Schema Deletion Suite", func(report ginkgo.Report) {
	if report.SuiteSucceeded {
		if stopFunc != nil {
			stopFunc()
		}
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		gomega.Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})
