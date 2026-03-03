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

package etcd_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/lifecycle"
)

func init() {
	lifecycle.SetupFunc = func() lifecycle.SetupResult {
		By("Starting etcd server")
		ep, _, etcdCleanup := setup.StartEmbeddedEtcd()
		By("Loading schema")
		setup.PreloadSchemaViaEtcd(ep, test_stream.LoadSchemaWithStages, test_measure.LoadSchemaWithStages,
			test_trace.PreloadSchemaWithStages, test_property.PreloadSchema)
		config := setup.EtcdClusterConfig(ep)
		By("Starting hot data node")
		dataAddr, srcDir, closeDataNode0 := setup.DataNodeWithAddrAndDir(config, "--node-labels", "type=hot",
			"--measure-flush-timeout", "0s", "--stream-flush-timeout", "0s", "--trace-flush-timeout", "0s")
		By("Starting warm data node")
		_, destDir, closeDataNode1 := setup.DataNodeWithAddrAndDir(config, "--node-labels", "type=warm",
			"--measure-flush-timeout", "0s", "--stream-flush-timeout", "0s", "--trace-flush-timeout", "0s")
		By("Starting liaison node")
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config, "--data-node-selector", "type=hot")
		By("Initializing test cases with 10 days before")
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		tenDaysBeforeNow := now.Add(-10 * 24 * time.Hour)
		test_cases.Initialize(liaisonAddr, tenDaysBeforeNow)
		time.Sleep(flags.ConsistentlyTimeout)
		return lifecycle.SetupResult{
			DataAddr:         dataAddr,
			LiaisonAddr:      liaisonAddr,
			Ep:               ep,
			SrcDir:           srcDir,
			DestDir:          destDir,
			TenDaysBeforeNow: tenDaysBeforeNow,
			MetadataFlags:    []string{"--etcd-endpoints", ep},
			StopFunc: func() {
				closerLiaisonNode()
				closeDataNode0()
				closeDataNode1()
				etcdCleanup()
			},
		}
	}
}

func TestEtcdLifecycle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Etcd Lifecycle Suite")
}
