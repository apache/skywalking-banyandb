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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/multi_segments"
)

func init() {
	multisegments.SetupFunc = func() multisegments.SetupResult {
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		Expect(tmpErr).NotTo(HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		config := setup.PropertyClusterConfig(dfWriter)
		By("Starting data node 0")
		closeDataNode0 := setup.DataNode(config)
		By("Starting data node 1")
		closeDataNode1 := setup.DataNode(config)
		By("Loading schema via property")
		setup.PreloadSchemaViaProperty(config, test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema, test_property.PreloadSchema)
		config.AddLoadedKinds(schema.KindStream, schema.KindMeasure, schema.KindTrace)
		By("Starting liaison node")
		liaisonAddr, closerLiaisonNode := setup.LiaisonNode(config)
		By("Initializing test cases")
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		baseTime := time.Date(now.Year(), now.Month(), now.Day(),
			0o0, 0o2, 0, 0, now.Location())
		test_cases.Initialize(liaisonAddr, baseTime)
		return multisegments.SetupResult{
			Addr:     liaisonAddr,
			Now:      now,
			BaseTime: baseTime,
			StopFunc: func() {
				closerLiaisonNode()
				closeDataNode0()
				closeDataNode1()
				tmpDirCleanup()
			},
		}
	}
}

func TestPropertyMultiSegments(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Property Multi Segments Suite")
}
