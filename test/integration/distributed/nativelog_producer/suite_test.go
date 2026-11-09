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

package nativelogproducer_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nativelog "github.com/apache/skywalking-banyandb/banyand/observability/logging"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases "github.com/apache/skywalking-banyandb/test/cases/nativelog"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestNativeLogProducer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Producer Suite", Label(integration_distributed.Labels...))
}

var (
	console *test.CapturedStderr
	stops   []func()
)

var _ = BeforeSuite(func() {
	var err error
	console, err = test.CaptureStderr()
	Expect(err).NotTo(HaveOccurred())
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())

	// The data node keeps the schema and collects nothing of its own; the
	// liaison is only the way rows are read.
	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	dataAddr, _, _, stopData := setup.DataNodeWithAddrAndDir(config, setup.ReceiverFlag, setup.FastSchemaSync)
	stops = append(stops, stopData)
	liaisonAddr, stopLiaison := setup.LiaisonNode(config, setup.FastSchemaSync)
	stops = append(stops, stopLiaison)

	conn, err := grpchelper.Conn(liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	stops = append(stops, func() { _ = conn.Close() })

	// Last, because the native sink is process-global.
	producerID, stopProducer := setup.StartNativeLogProducer("backup", nativelog.Destination{Addr: dataAddr})
	stops = append(stops, stopProducer)

	node := cases.Node{Conn: conn, NodeID: producerID, NodeType: "backup"}
	shared := func(_ ...string) (cases.Node, func()) { return node, func() {} }
	cases.SharedContext = cases.Context{
		Distributed:   true,
		Producer:      true,
		Start:         shared,
		StartDisabled: shared,
		Restart:       shared,
		Console:       console.String,
	}
	cases.AwaitQueryable(conn)
})

var _ = AfterSuite(func() {
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
	if console != nil {
		console.Restore()
	}
})

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
