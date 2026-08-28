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

// Package rbac holds the standalone integration proof of the liaison's global
// authorization boundary.
package rbac_test

import (
	"testing"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

// TestRBACGlobalWorkflow is the workflow-level gate of issue #14014. It drives the fixed
// admin/monitor/reader/writer/unbound matrix against a real standalone liaison over
// generated gRPC clients and the bound HTTP routes, and turns GREEN only once rounds
// A1-A4 and D1-D4 have all landed.
func TestRBACGlobalWorkflow(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "RBAC Global Workflow Suite", g.Label(integration_standalone.Labels...))
}
