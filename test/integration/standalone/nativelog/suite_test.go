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
	"fmt"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	cases "github.com/apache/skywalking-banyandb/test/cases/nativelog"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

func TestNativeLogStandalone(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Native Self-Stored Logs Standalone Suite", Label(integration_standalone.Labels...))
}

var stops []func()

var _ = BeforeSuite(func() {
	Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(Succeed())
	binPath := cases.RequireBinary()

	// Each producer is its own standalone server, with its own data: a
	// standalone serves only its own logs.
	native, _, stopNative := startStandalone(binPath, newSpace(), mustPorts(), cases.NativeFlags...)
	stops = append(stops, stopNative)
	disabled, _, stopDisabled := startStandalone(binPath, newSpace(), mustPorts(), cases.DisabledFlags...)
	stops = append(stops, stopDisabled)

	recoveryDir, recoveryPorts := newSpace(), mustPorts()
	cases.SharedContext = cases.Context{
		Native:   []cases.Producer{native},
		Disabled: disabled,
		Recovery: func(extraFlags ...string) (cases.Producer, string, func()) {
			return startStandalone(binPath, recoveryDir, recoveryPorts, append(append([]string{}, cases.NativeFlags...), extraFlags...)...)
		},
	}
})

var _ = AfterSuite(func() {
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
})

// startStandalone launches a standalone server on dataDir. ports holds the
// five the launcher needs and a sixth for the metrics listener: every server
// must have its own, or they would all bind the default one.
func startStandalone(binPath, dataDir string, ports []int, extraFlags ...string) (cases.Producer, string, func()) {
	// The launcher always writes <logDir>/standalone.log, so each server needs
	// a log directory of its own.
	logDir := newSpace()
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", ports[5])
	config := setup.PropertyClusterConfig(setup.NewDiscoveryFileWriter(newSpace()))
	grpcAddr, _, stop := setup.ExternalStandalone(config, binPath, dataDir, logDir, ports[:5],
		append([]string{"--observability-listener-addr=" + metricsAddr}, extraFlags...)...)
	conn, err := grpchelper.Conn(grpcAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	Expect(err).NotTo(HaveOccurred())
	producer := cases.Producer{
		Conn: conn,
		// The launcher sets --node-host=127.0.0.1, and a node's id is its
		// host and gRPC port.
		NodeID:   fmt.Sprintf("127.0.0.1:%d", ports[0]),
		NodeType: "standalone",
		LogPath:  filepath.Join(logDir, "standalone.log"),
	}
	return producer, "http://" + metricsAddr + "/metrics", func() {
		_ = conn.Close()
		stop()
	}
}

func mustPorts() []int {
	ports, err := test.AllocateFreePorts(6)
	Expect(err).NotTo(HaveOccurred())
	return ports
}

func newSpace() string {
	dir, _, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	return dir
}
