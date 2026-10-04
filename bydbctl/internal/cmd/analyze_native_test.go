// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package cmd

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestAnalyzeSeriesNativePopulated(t *testing.T) {
	path := t.TempDir()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: path})
	require.NoError(t, err)
	docs := make([]index.Document, 0, 3)
	for n, subject := range []string{"cpu", "cpu", "db"} {
		s := &pbv1.Series{Subject: subject, EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprint(n)}}}}}
		require.NoError(t, s.Marshal())
		docs = append(docs, index.Document{EntityValues: append([]byte(nil), s.Buffer...)})
	}
	require.NoError(t, store.InsertSeriesBatch(index.Batch{Documents: docs}))
	require.NoError(t, store.Close())
	before := treeHash(t, path)
	root := &cobra.Command{Use: "root"}
	RootCmdFlags(root)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"analyze", "series", path})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "cpu, 2")
	require.Contains(t, out.String(), "db, 1")
	require.Contains(t, out.String(), "total, 3")
	require.Equal(t, before, treeHash(t, path))
	filtered := &cobra.Command{Use: "root"}
	RootCmdFlags(filtered)
	var filteredOut bytes.Buffer
	filtered.SetOut(&filteredOut)
	filtered.SetErr(&filteredOut)
	filtered.SetArgs([]string{"analyze", "series", "--subject", "cpu", path})
	require.NoError(t, filtered.Execute())
	require.Contains(t, filteredOut.String(), "\"0\",")
	require.Contains(t, filteredOut.String(), "\"1\",")
	require.NotContains(t, filteredOut.String(), "\"2\",")
}

func TestAnalyzeSeriesEmptyDoesNotCreateFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-sidx")
	root := &cobra.Command{Use: "root"}
	RootCmdFlags(root)
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	root.SetArgs([]string{"analyze", "series", path})
	require.NoError(t, root.Execute())
	require.Equal(t, "total, 0\n", output.String())
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func treeHash(t *testing.T, root string) [32]byte {
	t.Helper()
	h := sha256.New()
	require.NoError(t, filepath.Walk(root, func(p string, i os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if i.IsDir() {
			return nil
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		_, _ = h.Write([]byte(p))
		_, _ = h.Write(b)
		return nil
	}))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
