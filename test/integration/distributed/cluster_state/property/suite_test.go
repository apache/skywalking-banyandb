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

package property_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/cluster_state"
)

func init() {
	clusterstate.SetupFunc = func() clusterstate.SetupResult {
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		Expect(tmpErr).NotTo(HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		By("Starting data node")
		dataAddr, srcDir, closeDataNode0 := setup.DataNodeWithAddrAndDir(config)
		By("Starting liaison node")
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config)
		return clusterstate.SetupResult{
			DataAddr:    dataAddr,
			LiaisonAddr: liaisonAddr,
			SrcDir:      srcDir,
			StopFunc: func() {
				closerLiaisonNode()
				closeDataNode0()
				tmpDirCleanup()
			},
		}
	}
}

func TestPropertyClusterState(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Property Get Cluster State Suite")
}
