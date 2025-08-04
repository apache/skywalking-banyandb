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

package gcs

import (
	"path"
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	remotegcs "github.com/apache/skywalking-banyandb/pkg/fs/remote/gcp"
	"github.com/apache/skywalking-banyandb/test/integration/distributed/backup"
	"github.com/apache/skywalking-banyandb/test/integration/dockertesthelper"
)

func TestBackup(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Distributed Backup Suite")
}

var testVars *backup.CommonTestVars

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	// Initialize distributed test environment
	var err error
	testVars, err = backup.InitializeTestSuite()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Launch fake GCS server
	if err = dockertesthelper.InitFakeGCSServer(); err != nil {
		ginkgo.By("Skipping GCS backup tests: unable to start fake-gcs-server: " + err.Error())
		ginkgo.Skip("fake-gcs-server unavailable")
	}

	// Create remote FS instance pointing to the emulator with basePath set to dest dir
	destDirName := filepath.Base(testVars.DestDir)
	fsPath := path.Join(dockertesthelper.GCSBucketName, destDirName)
	fs, err := remotegcs.NewFS(fsPath, &config.FsConfig{
		GCP: &config.GCPConfig{
			Bucket: dockertesthelper.GCSBucketName,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	testVars.FS = fs

	return []byte(testVars.DataAddr)
}, func(address []byte) {
	// Second function executed on all Ginkgo nodes
	var err error
	testVars.Connection, err = backup.SetupClientConnection(string(address))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	destDir := filepath.Base(testVars.DestDir)
	// Prepare shared context for backup/restore cases
	backup.SetupSharedContext(testVars,
		"gcs:///"+path.Join(dockertesthelper.GCSBucketName, destDir),
		nil)
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if testVars.Connection != nil {
		gomega.Expect(testVars.Connection.Close()).To(gomega.Succeed())
	}
	_ = dockertesthelper.CloseFakeGCSServer()
}, func() {
	backup.TeardownSuite(testVars)
})
