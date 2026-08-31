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

// Package rbacschema holds the standalone integration proof of the liaison's group-scoped
// schema authorization boundary.
package rbacschema_test

import (
	"testing"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	integration_standalone "github.com/apache/skywalking-banyandb/test/integration/standalone"
)

// TestRBACSchemaWorkflow is the workflow-level gate of issue #14015. It drives the whole
// group-scoped schema lifecycle — Group point reads and upserts, group deletion, the seven
// registry families, the filtered Group.List, and both SchemaBarrier scope forms — against a
// real standalone liaison over generated gRPC clients and the bound grpc-gateway routes, and
// turns GREEN only once rounds B1-B10 have all landed.
func TestRBACSchemaWorkflow(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "RBAC Schema Workflow Suite", g.Label(integration_standalone.Labels...))
}
