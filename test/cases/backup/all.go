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
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/backup"
)

var _ = ginkgo.Describe("Backup All", func() {
	_ = ginkgo.Describe("Backup and Restore Integration", func() {
		ginkgo.It("should backup, create timedir and restore data correctly", func() {
			destDir := SharedContext.DestDir
			destURL := SharedContext.DestURL
			fs := SharedContext.FS
			ginkgo.By("Backup data to a remote destination")
			defer os.RemoveAll(destDir)
			defer clearSnapshotDirs()

			backupCmd := backup.NewBackupCommand()
			backupCmd.SetArgs(append([]string{
				"--grpc-addr", SharedContext.DataAddr,
				"--stream-root-path", SharedContext.RootDir,
				"--measure-root-path", SharedContext.RootDir,
				"--property-root-path", SharedContext.RootDir,
				"--dest", destURL,
				"--time-style", "daily",
			}, SharedContext.S3Args...))

			err := backupCmd.Execute()
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var backupTimeDir string
			ctx := context.Background()
			entries, inErr := fs.List(ctx, "")
			gomega.Expect(inErr).NotTo(gomega.HaveOccurred())
			datePattern := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
			for _, entry := range entries {
				dirName := entry
				if slashIndex := strings.Index(entry, "/"); slashIndex > 0 {
					dirName = entry[:slashIndex]
				}
				if datePattern.MatchString(dirName) {
					backupTimeDir = dirName
					break
				}
			}
			gomega.Expect(backupTimeDir).NotTo(gomega.BeEmpty())

			ginkgo.By("List remote time directories")
			newCatalogDir, err := os.MkdirTemp("", "backup-restore-new-catalog")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer os.RemoveAll(newCatalogDir)

			listCmd := backup.NewTimeDirCommand()
			listCmd.SetArgs(append([]string{
				"list", "--dest", destURL,
			}, SharedContext.S3Args...))

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
			restoreCmd.SetArgs(append([]string{
				"run",
				"--source", destURL,
				"--stream-root-path", newCatalogDir,
				"--measure-root-path", newCatalogDir,
				"--property-root-path", newCatalogDir,
			}, SharedContext.S3Args...))
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
				restoredDataDir := filepath.Join(newCatalogDir, cat, "data")
				var remoteList []string
				var count int
				remoteDataDir := filepath.Join(latestTimedir, cat)
				remoteList, err = fs.List(context.Background(), remoteDataDir)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				count, err = countFilesRecursive(restoredDataDir)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(count).To(gomega.Equal(len(remoteList)))

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

func removeAllFilesInDir(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil
		}
		if info.IsDir() {
			return os.RemoveAll(path)
		}
		return os.Remove(path)
	})
}

func countFilesRecursive(dir string) (int, error) {
	var count int
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func clearSnapshotDirs() {
	snpDirs := []string{
		filepath.Join(SharedContext.RootDir, "measure", "snapshots"),
		filepath.Join(SharedContext.RootDir, "stream", "snapshots"),
		filepath.Join(SharedContext.RootDir, "property", "snapshots"),
	}

	for _, dir := range snpDirs {
		_ = removeAllFilesInDir(dir)
	}
}
