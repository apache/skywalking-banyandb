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

package nativelog_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases "github.com/apache/skywalking-banyandb/test/cases/nativelog"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestNativeLogDistributed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Distributed Suite", Label(integration_distributed.Labels...))
}

var stops []func()

// syncFlag shortens schema sync on every node. A node's watch of a schema
// server it finds later replays only revisions newer than the ones it already
// has, so it skips an older _monitoring_log schema that a native data node
// created before (apache/skywalking#14103). A full reconcile, every fifth
// sync, repairs that; the default 30s interval would take 150s.
const syncFlag = "--schema-property-client-sync-interval=1s"

var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())
	binPath := cases.RequireBinary()

	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	logDir := newSpace()

	// Every data node runs a schema server, so each needs four ports, and a
	// fifth for its own metrics listener.
	dn0 := startDataNode(config, binPath, newSpace(), logDir, mustPorts(5), cases.NativeFlags...)
	dn1 := startDataNode(config, binPath, newSpace(), logDir, mustPorts(5), cases.NativeFlags...)
	dn2 := startDataNode(config, binPath, newSpace(), logDir, mustPorts(5), cases.DisabledFlags...)

	lnPorts := mustPorts(4)
	lnAddr, lnStop := setup.ExternalLiaisonNode(config, binPath, newSpace(), logDir, lnPorts[:3],
		fmt.Sprintf("--observability-listener-addr=127.0.0.1:%d", lnPorts[3]), syncFlag)
	stops = append(stops, lnStop)
	conn, err := grpchelper.Conn(lnAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	stops = append(stops, func() { _ = conn.Close() })
	waitForDataNodes(conn, 3)
	cases.AwaitQueryable(conn)

	// Every case reads through the liaison.
	dn0.producer.Conn, dn1.producer.Conn, dn2.producer.Conn = conn, conn, conn

	recoveryDir, recoveryLogDir, recoveryPorts := newSpace(), newSpace(), mustPorts(5)
	cases.SharedContext = cases.Context{
		Distributed: true,
		Native:      []cases.Producer{dn0.producer, dn1.producer},
		Disabled:    dn2.producer,
		Recovery: func(extraFlags ...string) (cases.Producer, string, func()) {
			n := startDataNode(config, binPath, recoveryDir, recoveryLogDir, recoveryPorts,
				append(append([]string{}, cases.NativeFlags...), extraFlags...)...)
			// Its data must be reachable through the liaison before a case can
			// read it, or an empty read would prove nothing.
			waitForDataNodes(conn, 4)
			cases.AwaitQueryable(conn)
			n.producer.Conn = conn
			return n.producer, n.metricsURL, n.stop
		},
	}
})

var _ = AfterSuite(func() {
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
})

type dataNode struct {
	stop       func()
	metricsURL string
	producer   cases.Producer
}

// startDataNode launches a data node. ports holds the four the launcher needs
// and a fifth for the metrics listener: every node must have its own, or they
// would all bind the default one.
func startDataNode(config *setup.ClusterConfig, binPath, dataDir, logDir string, ports []int, extraFlags ...string) dataNode {
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[4])
	_, stop := setup.ExternalDataNode(config, binPath, dataDir, logDir, ports[:4],
		append([]string{"--observability-listener-addr=" + metricsAddr, syncFlag}, extraFlags...)...)
	stops = append(stops, stop)
	return dataNode{
		stop:       stop,
		metricsURL: "http://" + metricsAddr + "/metrics",
		producer: cases.Producer{
			// The launcher sets --node-host=127.0.0.1, and a node's id is its
			// host and gRPC port.
			NodeID:   fmt.Sprintf("127.0.0.1:%d", ports[0]),
			NodeType: "data",
			LogPath:  filepath.Join(logDir, fmt.Sprintf("data-%d.log", ports[0])),
		},
	}
}

// waitForDataNodes waits until the liaison routes to at least n data nodes.
func waitForDataNodes(conn *grpc.ClientConn, n int) {
	client := databasev1.NewClusterStateServiceClient(conn)
	Eventually(func(inner Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, err := client.GetClusterState(ctx, &databasev1.GetClusterStateRequest{})
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(len(state.GetRouteTables()["tire2"].GetActive())).To(BeNumerically(">=", n),
			"the liaison does not route to %d data nodes yet", n)
	}, flags.EventuallyTimeout, 500*time.Millisecond).Should(Succeed())
}

func mustPorts(n int) []int {
	ports, err := test.AllocateFreePorts(n)
	Expect(err).NotTo(HaveOccurred())
	return ports
}

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
