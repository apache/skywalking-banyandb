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

package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// HashTree fingerprints the part files and segment metadata below root: relative path ->
// content hash. The inverted index directories (sidx/, idx/) are skipped: their writers
// persist and merge on their own schedule, so their bytes move even when nobody reads them.
// Parts are immutable, which is what "planning never writes" can be asserted on.
func HashTree(root string) map[string]string {
	out, err := hashTree(root)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return out
}

// hashTree is HashTree returning the walk error. Entries removed mid-walk (a merge dropping
// a part) are skipped rather than failing the walk.
func hashTree(root string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) && p != root {
				if d != nil && d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "sidx" || d.Name() == "idx" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(d.Name()) == ".tmp" {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		body, err := os.ReadFile(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		out[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	return out, err
}

// TreeDiff lists the files whose presence or content differ between two fingerprints.
func TreeDiff(before, after map[string]string) []string {
	var diff []string
	for rel, h := range before {
		if got, ok := after[rel]; !ok {
			diff = append(diff, "removed "+rel)
		} else if got != h {
			diff = append(diff, "changed "+rel)
		}
	}
	for rel := range after {
		if _, ok := before[rel]; !ok {
			diff = append(diff, "added "+rel)
		}
	}
	sort.Strings(diff)
	return diff
}

// StableTree waits until two consecutive fingerprints of root agree and returns the last one.
func StableTree(root string) map[string]string {
	var snapshot map[string]string
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		first, err := hashTree(root)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		time.Sleep(time.Second)
		second, err := hashTree(root)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(TreeDiff(first, second)).To(gomega.BeEmpty(), "the data directory never went quiet")
		snapshot = second
	}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
	return snapshot
}
