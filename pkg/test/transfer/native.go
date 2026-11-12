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
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

// DefaultNativeExportOptions are DefaultExportOptions for a native export: no compression.
func DefaultNativeExportOptions(output string) config.Export {
	opts := DefaultExportOptions(output)
	opts.Format = transferv1.Format_FORMAT_NATIVE
	opts.Compress = transferv1.Compress_COMPRESS_NONE
	return opts
}

// NativeArtifact is a native artifact read back from disk.
type NativeArtifact struct {
	Manifest *manifest.Manifest
	// Containers maps the path prefix of every container, relative to the artifact, to its
	// volume paths in order.
	Containers map[string][]string
	Dir        string
	// Plain lists the files that are not containers, such as segment metadata files.
	Plain []string
}

// ReadNativeArtifact reads the manifest of a native artifact and checks it against the files
// on disk: importer.OpenArtifact must accept it with every data file verified, the artifact must
// be uncompressed native, the manifest must list exactly the data files on disk, and every
// shard container must hold no orphan file.
func ReadNativeArtifact(dir string) *NativeArtifact {
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, m.Format).To(gomega.Equal(manifest.FormatNative))
	gomega.ExpectWithOffset(1, m.Compress).To(gomega.Equal(manifest.CompressNone))
	_, err = importer.OpenArtifact(context.Background(), dir, true)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	listed := map[string]bool{}
	for files := range m.FileSets() {
		for _, f := range files {
			listed[f.Path] = true
		}
	}
	var onDisk []string
	nodes := filepath.Join(dir, "nodes")
	if _, statErr := os.Stat(nodes); statErr == nil {
		gomega.ExpectWithOffset(1, filepath.WalkDir(nodes, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return walkErr
			}
			rel, relErr := filepath.Rel(dir, p)
			if relErr != nil {
				return relErr
			}
			onDisk = append(onDisk, filepath.ToSlash(rel))
			return nil
		})).To(gomega.Succeed())
	}
	gomega.ExpectWithOffset(1, len(onDisk)).To(gomega.Equal(len(listed)), "the manifest lists every data file")
	a := &NativeArtifact{Dir: dir, Manifest: m, Containers: bnc.GroupVolumes(onDisk)}
	for _, rel := range onDisk {
		gomega.ExpectWithOffset(1, listed[rel]).To(gomega.BeTrue(), "%s is not in the manifest", rel)
		if _, _, isVolume := bnc.ParseVolumeName(rel); !isVolume {
			a.Plain = append(a.Plain, rel)
		}
	}
	sort.Strings(a.Plain)
	for prefix := range a.Containers {
		if strings.HasPrefix(path.Base(prefix), nativefmt.ShardDirPrefix) {
			set := a.Open(prefix)
			gomega.ExpectWithOffset(1, set.Shard().Orphans).To(gomega.BeEmpty(), prefix)
			gomega.ExpectWithOffset(1, set.Close()).To(gomega.Succeed())
		}
	}
	return a
}

// Open opens the volumes of the container at prefix as one volume set.
func (a *NativeArtifact) Open(prefix string) *bnc.VolumeSet {
	vols := a.Containers[prefix]
	gomega.ExpectWithOffset(1, vols).NotTo(gomega.BeEmpty(), prefix)
	paths := make([]string, len(vols))
	for i, v := range vols {
		paths[i] = filepath.Join(a.Dir, filepath.FromSlash(v))
	}
	set, err := bnc.OpenVolumes(paths)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), prefix)
	return set
}

// RequireNativePartNames pins nativefmt's disk-to-wire names of catalog's parts to the engine:
// every file of the part flushed at partDir must map to a name of wire, the files the engine's
// sync sender streams for it, or be skipped, and every name of wire must be mapped; each of
// skipped, a file the part may lack, must be skipped too. A renamed or new part file then fails
// the engine's tests instead of an import.
func RequireNativePartNames(t testing.TB, catalog commonv1.Catalog, partDir string, wire []queue.FileInfo, skipped ...string) {
	t.Helper()
	entries, err := os.ReadDir(partDir)
	require.NoError(t, err)
	want := make([]string, len(wire))
	for i, f := range wire {
		want[i] = f.Name
	}
	var got []string
	for _, e := range entries {
		name, skip, mapErr := nativefmt.SyncFileName(catalog, nativefmt.KindPart, "", e.Name())
		require.NoError(t, mapErr, e.Name())
		if !skip {
			got = append(got, name)
		}
	}
	require.ElementsMatch(t, want, got, "the wire names of the files in %s", partDir)
	for _, disk := range skipped {
		_, skip, mapErr := nativefmt.SyncFileName(catalog, nativefmt.KindPart, "", disk)
		require.NoError(t, mapErr, disk)
		require.True(t, skip, disk)
	}
}
