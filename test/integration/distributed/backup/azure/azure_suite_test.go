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

package azure

import (
	"os"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	remoteazure "github.com/apache/skywalking-banyandb/pkg/fs/remote/azure"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/backup"
	"github.com/apache/skywalking-banyandb/test/integration/dockertesthelper"
)

var testVars *backup.CommonTestVars

func TestBackup(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Distributed Backup Suite")
}

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	var err error
	testVars, err = backup.InitializeTestSuite()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Start Azurite container via helper
	ginkgo.By("Starting Azurite container")
	err = dockertesthelper.InitAzuriteContainer()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Set connection string env var for azure SDK
	err = os.Setenv("AZURE_STORAGE_CONNECTION_STRING", dockertesthelper.AzuriteConnStr)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Create FS with proper path format (no leading slash)
	// The path should be just the subdirectory within the container
	fs, err := remoteazure.NewFS(testVars.DestDir, &config.FsConfig{
		Azure: &config.AzureConfig{
			Container: dockertesthelper.AzuriteContainer,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	testVars.FS = fs

	return []byte(testVars.DataAddr)
}, func(address []byte) {
	var err error
	testVars.Connection, err = backup.SetupClientConnection(string(address))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Azure URL format: azure:///<container>/<path>
	backup.SetupSharedContext(testVars,
		"azure:///"+dockertesthelper.AzuriteContainer+"/"+testVars.DestDir,
		nil)
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if testVars.Connection != nil {
		gomega.Expect(testVars.Connection.Close()).To(gomega.Succeed())
	}
	gomega.Expect(dockertesthelper.CloseAzuriteContainer()).To(gomega.Succeed())
}, func() {
	backup.TeardownSuite(testVars)
})
