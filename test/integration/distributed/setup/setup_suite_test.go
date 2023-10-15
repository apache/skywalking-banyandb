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

// Package integration_setup_test is a integration test suite.
package integration_setup_test

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/banyand/metadata/embeddedetcd"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestSetup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Setup Suite")
}

var (
	deferFunc    func()
	goods        []gleak.Goroutine
	etcdEndpoint string
)

var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})).To(Succeed())
	goods = gleak.Goroutines()
	ports, err := test.AllocateFreePorts(2)
	Expect(err).NotTo(HaveOccurred())
	dir, spaceDef, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	ep := fmt.Sprintf("http://127.0.0.1:%d", ports[0])
	server, err := embeddedetcd.NewServer(
		embeddedetcd.ConfigureListener([]string{ep}, []string{fmt.Sprintf("http://127.0.0.1:%d", ports[1])}),
		embeddedetcd.RootDir(dir))
	Expect(err).ShouldNot(HaveOccurred())
	<-server.ReadyNotify()
	deferFunc = func() {
		_ = server.Close()
		<-server.StopNotify()
		spaceDef()
	}
	return []byte(ep)
}, func(ep []byte) {
	etcdEndpoint = string(ep)
})

var _ = SynchronizedAfterSuite(func() {}, func() {
	deferFunc()
	Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
})
