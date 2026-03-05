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

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
	"github.com/apache/skywalking-banyandb/test/integration/standalone/other"
)

func init() {
	other.SetupFunc = func() other.SetupResult {
		tmpDir, tmpDirCleanup, tmpErr := test.NewSpace()
		gm.Expect(tmpErr).NotTo(gm.HaveOccurred())
		dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
		g.DeferCleanup(tmpDirCleanup)
		return other.SetupResult{
			Config: setup.PropertyClusterConfig(dfWriter),
		}
	}
}

func TestPropertyIntegrationOther(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "Integration Property Other Suite", g.Label(integration_standalone.Labels...))
}
