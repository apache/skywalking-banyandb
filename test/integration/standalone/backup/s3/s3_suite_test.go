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

package s3

import (
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/fs/remote/aws"
	"github.com/apache/skywalking-banyandb/pkg/fs/remote/config"
	"github.com/apache/skywalking-banyandb/test/integration/dockertesthelper"
	"github.com/apache/skywalking-banyandb/test/integration/standalone/backup"
)

func TestBackup(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Backup Suite", ginkgo.Label(backup.GetTestLabels()...))
}

var testVars *backup.CommonTestVars

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	var err error
	var addr string
	testVars, addr, err = backup.InitStandaloneEnv()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	err = dockertesthelper.InitMinIOContainer()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	fs, err := aws.NewFS(filepath.Join(dockertesthelper.BucketName, testVars.DestDir), &config.FsConfig{
		S3: &config.S3Config{
			S3ConfigFilePath:     dockertesthelper.S3ConfigPath,
			S3CredentialFilePath: dockertesthelper.S3CredentialsPath,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	testVars.FS = fs

	return []byte(addr)
}, func(address []byte) {
	addr := string(address)
	err := backup.SetupConnection(testVars, addr,
		"s3:///"+dockertesthelper.BucketName+testVars.DestDir,
		[]string{
			"--s3-credential-file", dockertesthelper.S3CredentialsPath,
			"--s3-config-file", dockertesthelper.S3ConfigPath,
		})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	if testVars.Connection != nil {
		gomega.Expect(testVars.Connection.Close()).To(gomega.Succeed())
	}
	dockertesthelper.CloseMinioContainer()
}, func() {
	backup.TeardownSuite(testVars)
})
