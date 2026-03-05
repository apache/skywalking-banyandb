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

	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/schema"
)

func init() {
	schema.SetupFunc = func() schema.SetupResult {
		By("Starting etcd server")
		ep, _, etcdCleanup := setup.StartEmbeddedEtcd()
		By("Loading schema")
		setup.PreloadSchemaViaEtcd(ep, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema)
		config := setup.EtcdClusterConfig(ep)
		By("Starting data node 0")
		closeDataNode0 := setup.DataNode(config)
		By("Starting data node 1")
		closeDataNode1 := setup.DataNode(config)
		By("Starting liaison node")
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config)
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		return schema.SetupResult{
			Addr: liaisonAddr,
			Now:  now,
			StopFunc: func() {
				closerLiaisonNode()
				closeDataNode0()
				closeDataNode1()
				etcdCleanup()
			},
		}
	}
}

func TestEtcdSchemaDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Etcd Schema Deletion Suite")
}
