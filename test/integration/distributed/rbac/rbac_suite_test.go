// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package rbac_test

import (
	"testing"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestRBACDistributedWorkflow(t *testing.T) {
	gm.RegisterFailHandler(g.Fail)
	g.RunSpecs(t, "RBAC Distributed Data Workflow Suite", g.Label(integration_distributed.Labels...))
}
