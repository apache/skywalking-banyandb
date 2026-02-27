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

package integration_schema_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/gmatcher"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	casesschema "github.com/apache/skywalking-banyandb/test/cases/schema"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestSchemaDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Standalone Schema Deletion Suite", Label(integration_standalone.Labels...))
}

var (
	connection *grpc.ClientConn
	deferFunc  func()
	goods      []gleak.Goroutine
)

var _ = SynchronizedBeforeSuite(func() []byte {
	goods = gleak.Goroutines()
	pool.EnableStackTracking(true)
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	addr, _, closeFn := setup.EmptyStandalone()
	deferFunc = closeFn
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpchelper.Conn(string(address), 10*time.Second,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	casesschema.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   time.Now(),
	}
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {})

var _ = ReportAfterSuite("Standalone Schema Deletion Suite", func(report Report) {
	if report.SuiteSucceeded {
		if deferFunc != nil {
			deferFunc()
		}
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
		Eventually(pool.AllRefsCount, flags.EventuallyTimeout).Should(gmatcher.HaveZeroRef())
	}
})
