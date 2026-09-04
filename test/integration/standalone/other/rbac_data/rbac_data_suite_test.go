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

// Package rbacdata holds the standalone end-to-end proof of the liaison's data and
// special-path authorization boundary.
package rbacdata_test

import (
	"testing"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

// TestRBACDataWorkflow is the workflow-level gate of issue #14016 and the closing gate of
// #13994. It drives a real deployment of the feature end to end against a real standalone
// liaison over generated gRPC clients and the bound grpc-gateway routes: an administrator
// seeds group-unique marker data through the protected write API, a group-scoped tenant reads
// its own group and is refused every other one, a group-scoped writer's forbidden frame is
// rejected mid-stream and leaves nothing behind, property mutations are refused without a side
// effect, and a ByDBQL query is decided by the native request it transformed into rather than
// by its text.
//
// It turns GREEN only once rounds C1-C8 have all landed.
func TestRBACDataWorkflow(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "RBAC Data Workflow Suite", g.Label(integration_standalone.Labels...))
}
