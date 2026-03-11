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

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	test_cases "github.com/apache/skywalking-banyandb/test/cases"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
	"github.com/apache/skywalking-banyandb/test/integration/standalone/query_ondisk"
)

func init() {
	queryondisk.SetupFunc = func() queryondisk.SetupResult {
		path, diskCleanupFn, pathErr := test.NewSpace()
		Expect(pathErr).NotTo(HaveOccurred())
		var ports []int
		ports, portsErr := test.AllocateFreePorts(4)
		Expect(portsErr).NotTo(HaveOccurred())
		addr, _, closeFunc := setup.ClosableStandalone(nil, path, ports)
		ns := timestamp.NowMilli().UnixNano()
		now := time.Unix(0, ns-ns%int64(time.Minute))
		test_cases.Initialize(addr, now)
		time.Sleep(5 * time.Second)
		closeFunc()
		time.Sleep(time.Second)
		addr, _, closeFunc = setup.EmptyClosableStandalone(nil, path, ports)
		return queryondisk.SetupResult{
			Addr: addr,
			Now:  now,
			StopFunc: func() {
				closeFunc()
				diskCleanupFn()
			},
		}
	}
}

func TestEtcdIntegrationQueryOnDisk(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Query OnDisk Suite", Label(integration_standalone.Labels...))
}
