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
	"bytes"
	"hash/crc32"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// CopyWithGroup copies the artifact src to dst with the schema of group changed by edit,
// keeping its manifest consistent, and returns dst.
func CopyWithGroup(src, dst, group string, edit func(*commonv1.Group)) string {
	gomega.ExpectWithOffset(1, filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), raw, 0o600)
	})).To(gomega.Succeed())
	rel := path.Join(schemasync.Dir, schemasync.KindGroup+".jsonl")
	file := filepath.Join(dst, filepath.FromSlash(rel))
	raw, err := os.ReadFile(file)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var out bytes.Buffer
	found := false
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		g := &commonv1.Group{}
		gomega.ExpectWithOffset(1, protojson.Unmarshal(line, g)).To(gomega.Succeed())
		if g.GetMetadata().GetName() == group {
			edit(g)
			found = true
		}
		line, err = protojson.Marshal(g)
		gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
		out.Write(line)
		out.WriteByte('\n')
	}
	gomega.ExpectWithOffset(1, found).To(gomega.BeTrue(), "the artifact has group %s", group)
	gomega.ExpectWithOffset(1, os.WriteFile(file, out.Bytes(), 0o600)).To(gomega.Succeed())
	manifestPath := filepath.Join(dst, manifest.FileName)
	m, err := manifest.Read(manifestPath)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	for i := range m.SchemaFiles {
		if m.SchemaFiles[i].Path == rel {
			m.SchemaFiles[i].Bytes = uint64(out.Len())
			m.SchemaFiles[i].CRC32 = manifest.FormatCRC32(crc32.ChecksumIEEE(out.Bytes()))
		}
	}
	gomega.ExpectWithOffset(1, manifest.Write(manifestPath, m)).To(gomega.Succeed())
	return dst
}
