// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to You under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
// ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

package rbacschema_test

import (
	"net/http"
	"os"
	"path/filepath"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

var _ = g.Describe("rbac-schema HTTP malformed request", func() {
	var (
		httpAddr string
		cleanup  func()
	)

	g.BeforeEach(func() {
		dataRoot, releaseSpace, spaceErr := test.NewSpace()
		gm.Expect(spaceErr).NotTo(gm.HaveOccurred())

		policyFile := filepath.Join(dataRoot, "security.yaml")
		writeErr := os.WriteFile(policyFile, []byte(schemaPolicy), 0o600)
		gm.Expect(writeErr).NotTo(gm.HaveOccurred())

		ports, portErr := test.AllocateFreePorts(5)
		gm.Expect(portErr).NotTo(gm.HaveOccurred())

		_, assignedHTTPAddr, closeServer := setup.EmptyClosableStandalone(nil, dataRoot, ports,
			"--auth-config-file="+policyFile)
		httpAddr = assignedHTTPAddr
		cleanup = func() {
			closeServer()
			releaseSpace()
		}
	})

	g.AfterEach(func() {
		cleanup()
	})

	g.It("returns HTTP 400 for an authenticated malformed schema request", func() {
		statusCode, responseBody := httpCall(httpAddr, http.MethodPost, "/api/v1/measure/schema", writerAlphaActor, "{}")
		gm.Expect(statusCode).To(gm.Equal(http.StatusBadRequest),
			"a nil measure body must stay malformed over HTTP: %s", responseBody)
	})
})
