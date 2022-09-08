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
package stress_test

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/z"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	"github.com/apache/skywalking-banyandb/pkg/test/measure"
	measure_traffic "github.com/apache/skywalking-banyandb/pkg/test/measure/traffic"
	"github.com/apache/skywalking-banyandb/test/cases/stream_tests"
)

const metricNum = 3 * 500

var (
	failInterceptor   *helpers.FailInterceptor
	banyandSession    *gexec.Session
	rootPath          string
	deferRootPathFunc func()
	// streamTrafficSenderCloser  *z.Closer
	measureTrafficSenderCloser *z.Closer
)

func TestStress(t *testing.T) {
	t.Skip("skip stress test")
	failInterceptor = helpers.NewFailInterceptor(Fail)
	RegisterFailHandler(failInterceptor.Fail)
	RunSpecs(t, "Stress Suite")
}

var _ = SynchronizedBeforeSuite(func() []byte {
	banyandBinary, err := gexec.Build("github.com/apache/skywalking-banyandb/banyand/cmd/server")
	Expect(err).ShouldNot(HaveOccurred())

	rootPath, deferRootPathFunc, err = test.NewSpace()
	Expect(err).NotTo(HaveOccurred())

	cmd := exec.Command(banyandBinary,
		"standalone",
		"--stream-root-path="+rootPath,
		"--measure-root-path="+rootPath,
		"--metadata-root-path="+rootPath,
	)
	banyandSession, err = gexec.Start(cmd, os.Stdout, os.Stdout)
	Expect(err).ShouldNot(HaveOccurred())

	address := "localhost:17912"

	Eventually(helpers.HealthCheck(address, 30*time.Second, time.Second)).Should(Succeed())
	// Expect(stream.RegisterForNew(address)).ShouldNot(HaveOccurred())
	Expect(measure.RegisterForNew(address, metricNum)).ShouldNot(HaveOccurred())
	// streamTrafficSenderCloser, err = stream_traffic.SendWrites(stream_traffic.TestCase{
	// 	Addr:                address,
	// 	SvcNum:              3,
	// 	InstanceNumEverySvc: 10,
	// })
	// Expect(err).ShouldNot(HaveOccurred())
	measureTrafficSenderCloser, err = measure_traffic.SendWrites(measure_traffic.TestCase{
		Addr:                address,
		SvcNum:              3,
		InstanceNumEverySvc: 10,
		MetricNum:           metricNum,
	})
	Expect(err).ShouldNot(HaveOccurred())
	return []byte(address)
}, func(address []byte) {
	stream_tests.Addr = string(address)
})

var _ = BeforeEach(func() {
	failInterceptor.Reset()
})

var _ = AfterEach(func() {
	if failInterceptor.DidFail() {
		fmt.Printf("TEST JUST FAILED: %s", CurrentGinkgoTestDescription().FullTestText)
	}
	// pause the suite to investigate the banyand server
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT)
	sig := <-s
	fmt.Printf("%s signal received", sig)

})

var _ = SynchronizedAfterSuite(func() {
}, func() {
	// streamTrafficSenderCloser.SignalAndWait()
	if measureTrafficSenderCloser != nil {
		measureTrafficSenderCloser.SignalAndWait()
	}
	banyandSession.Kill().Wait()
	deferRootPathFunc()
	gexec.CleanupBuildArtifacts()
})
