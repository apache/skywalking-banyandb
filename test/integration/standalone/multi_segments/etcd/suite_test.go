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

	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
	"github.com/apache/skywalking-banyandb/test/integration/standalone/multi_segments"
)

func init() {
	multisegments.SetupFunc = func() multisegments.SetupResult {
		addr, _, cleanup := setup.Standalone(nil)
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		baseTime := time.Date(now.Year(), now.Month(), now.Day(), 0o0, 0o2, 0, 0, now.Location())
		test_cases.Initialize(addr, baseTime)
		return multisegments.SetupResult{
			Addr:     addr,
			Now:      now,
			BaseTime: baseTime,
			StopFunc: cleanup,
		}
	}
}

func TestEtcdIntegrationMultiSegments(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Multi Segments Suite", Label(integration_standalone.Labels...))
}
