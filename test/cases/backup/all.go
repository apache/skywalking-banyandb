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

package backup_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/backup"
)

var _ = ginkgo.Describe("Backup All", func() {
	_ = ginkgo.Describe("Backup and Restore Integration", func() {
		ginkgo.It("should backup, create timedir and restore data correctly", func() {
			ginkgo.By("Backup data to a remote destination")
			destDir, err := os.MkdirTemp("", "backup-restore-dest")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer os.RemoveAll(destDir)
			destURL := "file://" + destDir

			backupCmd := backup.NewBackupCommand()
			backupCmd.SetArgs([]string{
				"--grpc-addr", SharedContext.DataAddr,
				"--stream-root-path", SharedContext.RootDir,
				"--measure-root-path", SharedContext.RootDir,
				"--property-root-path", SharedContext.RootDir,
				"--dest", destURL,
				"--time-style", "daily",
			})
			err = backupCmd.Execute()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var backupTimeDir string
			entries, err := os.ReadDir(destDir)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, entry := range entries {
				if entry.IsDir() {
					backupTimeDir = entry.Name()
					break
				}
			}
			gomega.Expect(backupTimeDir).NotTo(gomega.BeEmpty())

			ginkgo.By("List remote time directories")
			newCatalogDir, err := os.MkdirTemp("", "backup-restore-new-catalog")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer os.RemoveAll(newCatalogDir)

			listCmd := backup.NewTimeDirCommand()
			listCmd.SetArgs([]string{"list", "--dest", destURL})
			listOut := &bytes.Buffer{}
			listCmd.SetOut(listOut)
			listCmd.SetErr(listOut)
			err = listCmd.Execute()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Parse the list command output.
			// Example expected output:
			//   Remote time directories:
			//   2025-02-12
			outputLines := strings.Split(listOut.String(), "\n")
			var latestTimedir string
			for _, line := range outputLines {
				trim := strings.TrimSpace(line)
				if trim != "" && !strings.HasPrefix(trim, "Remote time directories:") {
					latestTimedir = trim
					break
				}
			}
			gomega.Expect(latestTimedir).To(gomega.Equal(backupTimeDir))

			ginkgo.By("Create timedir in new catalog's root path")
			createCmd := backup.NewTimeDirCommand()
			createCmd.SetArgs([]string{
				"create",
				"--stream-root", newCatalogDir,
				"--measure-root", newCatalogDir,
				"--property-root", newCatalogDir,
				latestTimedir,
			})
			createOut := &bytes.Buffer{}
			createCmd.SetOut(createOut)
			createCmd.SetErr(createOut)
			err = createCmd.Execute()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Read timedir from new catalog's root path")
			readCmd := backup.NewTimeDirCommand()
			readCmd.SetArgs([]string{
				"read",
				"--stream-root", newCatalogDir,
				"--measure-root", newCatalogDir,
				"--property-root", newCatalogDir,
			})
			readOut := &bytes.Buffer{}
			readCmd.SetOut(readOut)
			readCmd.SetErr(readOut)
			err = readCmd.Execute()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			readResult := readOut.String()
			gomega.Expect(readResult).To(gomega.ContainSubstring(latestTimedir))

			ginkgo.By("Create random files in the data directories of the new catalog")
			catalogs := []string{"stream", "measure", "property"}
			for _, cat := range catalogs {
				dataDir := filepath.Join(newCatalogDir, cat, "data")
				err = os.MkdirAll(dataDir, 0o755)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				randomFile := filepath.Join(dataDir, "random.txt")
				err = os.WriteFile(randomFile, []byte("some random data"), 0o600)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}

			ginkgo.By("Restore data from the remote destination")
			restoreCmd := backup.NewRestoreCommand()
			restoreCmd.SetArgs([]string{
				"run",
				"--source", destURL,
				"--stream-root-path", newCatalogDir,
				"--measure-root-path", newCatalogDir,
				"--property-root-path", newCatalogDir,
			})
			restoreOut := &bytes.Buffer{}
			restoreCmd.SetOut(restoreOut)
			restoreCmd.SetErr(restoreOut)
			err = restoreCmd.Execute()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			// Verify that the random files are removed and the data from remote backup is restored.
			for _, cat := range catalogs {
				// The extra file should have been removed.
				randomFile := filepath.Join(newCatalogDir, cat, "data", "random.txt")
				_, err = os.Stat(randomFile)
				gomega.Expect(os.IsNotExist(err)).To(gomega.BeTrue())

				// Verify that the restored files exist.
				// The remote backup data for each catalog is under: destDir/<latestTimedir>/<catalog>
				remoteDataDir := filepath.Join(destDir, latestTimedir, cat, "data")
				restoredDataDir := filepath.Join(newCatalogDir, cat, "data")
				var remoteEntries, restoredEntries []os.DirEntry
				remoteEntries, err = os.ReadDir(remoteDataDir)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				restoredEntries, err = os.ReadDir(restoredDataDir)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(len(restoredEntries)).To(gomega.Equal(len(remoteEntries)))
			}

			// Verify that the timedir file is removed from the new catalog's root path (after a successful restore).
			for _, cat := range catalogs {
				timedirFile := filepath.Join(newCatalogDir, cat, "time-dir")
				_, err = os.Stat(timedirFile)
				gomega.Expect(os.IsNotExist(err)).To(gomega.BeTrue())
			}
		})
	})
})
