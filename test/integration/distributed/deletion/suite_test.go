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

package deletion_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/deletion"
)

func init() {
	deletion.SetupFunc = func() deletion.SetupResult {
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		Expect(tmpErr).NotTo(HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		By("Starting data node 0")
		_, dn0Path, closeDataNode0 := setup.DataNodeWithAddrAndDir(config)
		By("Starting data node 1")
		_, dn1Path, closeDataNode1 := setup.DataNodeWithAddrAndDir(config)
		By("Starting liaison node")
		liaisonAddr, liaisonPath, closerLiaisonNode := setup.LiaisonNodeWithAddrAndDir(config)
		return deletion.SetupResult{
			LiaisonAddr:     liaisonAddr,
			DataNode0Path:   dn0Path,
			DataNode1Path:   dn1Path,
			LiaisonNodePath: liaisonPath,
			StopFunc: func() {
				closerLiaisonNode()
				closeDataNode0()
				closeDataNode1()
				tmpDirCleanup()
			},
		}
	}
}

func TestDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Deletion Suite")
}
